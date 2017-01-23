
import argparse
import os
import sys
from glob import glob
import json
from datetime import datetime

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
from RunTools import RunTools
from libs.databaseconnection import DatabaseConnection
import libs.files
from libs.config import get_seasons_info
from libs.logger import DummyLogger

parser = argparse.ArgumentParser()
parser.add_argument('--season', help = "The season, e.g. 2011", type = int)
parser.add_argument('--outfile', help = "Output JSON file")
parser.add_argument('-n', nargs = "?", help = "Number of runs that should be processed", type = int)
parser.add_argument('--src', help = "Data source can be: L2, PFDST, PFFilt", type = str)
args = parser.parse_args()

def get_runs_from_live(season, db):
    seasons = get_seasons_info()

    first_run = seasons[season]['first']
    last_run = seasons[season + 1]['first'] - 1
    excluded_runs = seasons[season + 1]['test']
    included_runs = seasons[season]['test']

    livesql = """SELECT r.runNumber,r.tStart,r.tStop,
                    r.tStart_frac,r.tStop_frac,r.nEvents,r.rateHz,
                    l.snapshot_id,l.good_i3,l.good_it,l.reason_i3,l.reason_it,
                    l.good_tstart, l.good_tstart_frac, l.good_tstop,l.good_tstop_frac,
                    r.grl_start_time,r.grl_start_time_frac,grl_stop_time,grl_stop_time_frac
                 FROM live.livedata_snapshotrun l
                 JOIN live.livedata_run r
                    ON l.run_id=r.id
                 WHERE (r.runNumber>=%s OR r.runNumber IN (%s))
                    AND r.runNumber<=%s
                    AND r.runNumber NOT IN (%s)
                 ORDER BY l.snapshot_id""" % (first_run,
                        ','.join([str(r) for r in included_runs] + ['-1']),
                        last_run,
                        ','.join(str(r) for r in excluded_runs))

    return db.fetchall(livesql, UseDict = True)

def get_first_event_time_from_file(path):
    from icecube import dataio, dataclasses

    f = dataio.I3File(path)
    
    while f.more():
        frame = f.pop_frame()
        if 'I3EventHeader' in frame.keys():
            return frame['I3EventHeader'].start_time.date_time

    raise

def get_last_event_time_from_file(path):
    from icecube import dataio, dataclasses

    f = dataio.I3File(path)
    
    last_time = None

    while f.more():
        frame = f.pop_frame()
        if 'I3EventHeader' in frame.keys():
            last_time = frame['I3EventHeader'].end_time.date_time

    if last_time is None:
        raise
    else:
        return last_time

def check_files_and_times(run_data, src, summarydict):
    tools = None

    if src == 'PFDST':
        tools = RunTools(run_data['runNumber'], passNumber = 2)
    else:
        tools = RunTools(run_data['runNumber'])

    file_type = 'P'

    if src == 'L2':
        file_type = 'L'

    infiles = tools.GetRunFiles(run_data['tStart'], file_type)

    if src == 'L2':
        infiles = [f for f in infiles if 'GCD' not in f and 'IT' not in f and 'EHE' not in f and 'SLOP' not in f and 'gaps' not in f and '.root' not in f]
        infiles.sort()

        print infiles

        summarydict[run_data['runNumber']] = {'missing_files': [], 'metadata_start_time': None, 'metadata_stop_time': None}

        if len(infiles):
            summarydict[run_data['runNumber']]['metadata_start_time'] = get_first_event_time_from_file(infiles[0])
            summarydict[run_data['runNumber']]['metadata_stop_time'] = get_last_event_time_from_file(infiles[-1])
        else:
            summarydict[run_data['runNumber']]['missing_files'] = [-1]
    else:
        tools.FilesComplete(infiles, run_data, outdict = summarydict)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial

    raise TypeError ("Type not serializable")

slogger = DummyLogger()
slogger.silence = True
db = DatabaseConnection.get_connection('i3live', slogger)

data = get_runs_from_live(args.season, db)

summary = {}

counter = 0
for run_data in data:
    print "%s\tCheck run %s" % (counter + 1, run_data['runNumber'])

    check_files_and_times(run_data, args.src, summary)

    counter = counter + 1
    if args.n is not None and counter >= args.n:
        break

with open(args.outfile, 'w') as f:
    json.dump(summary, f, default = json_serial)


