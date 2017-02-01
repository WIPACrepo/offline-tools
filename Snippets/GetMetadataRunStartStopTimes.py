
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

def get_gaps_file_data(db, runs):
    sql = "SELECT * FROM sub_runs WHERE run_id IN (%s)" % ','.join([str(r) for r in runs])

    dbdata = db.fetchall(sql, UseDict = True)

    data = {}
    for row in dbdata:
        if row['run_id'] not in data:
            data[row['run_id']] = {}

        data[row['run_id']][row['sub_run']] = row

    return data

def check_files_and_times(run_data, src, summarydict, gaps_data):
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
        from icecube import dataclasses

        summarydict[run_data['runNumber']] = {'missing_files': [], 'metadata_start_time': None, 'metadata_stop_time': None}

        if run_data['runNumber'] in gaps_data:
            first_sr = min(gaps_data[run_data['runNumber']].keys())
            last_sr = max(gaps_data[run_data['runNumber']].keys())

            summarydict[run_data['runNumber']]['metadata_start_time'] = dataclasses.I3Time(gaps_data[run_data['runNumber']][first_sr]['first_event_year'], gaps_data[run_data['runNumber']][first_sr]['first_event_frac']).date_time
            summarydict[run_data['runNumber']]['metadata_stop_time'] = dataclasses.I3Time(gaps_data[run_data['runNumber']][last_sr]['last_event_year'], gaps_data[run_data['runNumber']][last_sr]['last_event_frac']).date_time
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
filter_db = DatabaseConnection.get_connection('filter-db', slogger)

data = get_runs_from_live(args.season, db)

run_id_list = [d['runNumber'] for d in data]

gaps_data = get_gaps_file_data(filter_db, run_id_list)

summary = {}

counter = 0
for run_data in data:
    print "%s\tCheck run %s" % (counter + 1, run_data['runNumber'])

    check_files_and_times(run_data, args.src, summary, gaps_data)

    counter = counter + 1
    if args.n is not None and counter >= args.n:
        break

with open(args.outfile, 'w') as f:
    json.dump(summary, f, default = json_serial)


