
import sys
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
from RunTools import RunTools
import libs.databaseconnection
import libs.logger
import matplotlib
matplotlib.use('Agg') 
import pylab
import json
import datetime
import argparse
from glob import glob

def get_incompleted_pfdst_runs(filename):
    runs = set()
    with open(filename, 'r') as f:
        for l in f:
            runs.add(int(l.split('_Run00')[1].split('_')[0]))

    return runs

def get_run_info_from_live(db, runs):
    sql = """SELECT 
    runNumber AS `run_id`,
    snapshot_id,
    IFNULL(good_tstart, tstart) AS tstart,
    IFNULL(good_tstop, tstop) AS tstop
FROM
    live.livedata_snapshotrun s
        JOIN
    live.livedata_run r ON r.id = s.run_id
WHERE runNumber IN (%s) AND (good_it OR good_i3)
ORDER BY run_id, snapshot_id""" % ','.join([str(r) for r in runs])

    dbresult = db.fetchall(sql, UseDict = True)

    result = {}

    for row in dbresult:
        result[row['run_id']] = row

    return result

def parse_date(val):
    try:
        return datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%f")
    except:
        return datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")

def fix_file_data_dates(data):
    for run_id, value in data.iteritems():
        if value['metadata_stop_time'] is not None:
            value['metadata_stop_time'] = parse_date(value['metadata_stop_time'])
        if value['metadata_start_time'] is not None:
            value['metadata_start_time'] = parse_date(value['metadata_start_time'])

def create_start_hist(incompleted_runs, file_data, db_data, filename):
    diff = []

    for run_id in db_data.keys():
        if run_id in incompleted_runs:
            continue

        fdata = file_data[str(run_id)]

        if not (fdata['metadata_stop_time'] is not None and fdata['metadata_start_time'] is not None and len(fdata['missing_files']) == 0):
            continue

        ddata = db_data[int(run_id)]

        diff.append((fdata['metadata_start_time'] - ddata['tstart']).total_seconds())

        if diff[-1] > 2 or diff[-1] < 0:
            tools = RunTools(run_id, passNumber = 2)
            pfilenames = tools.GetRunFiles(ddata['tstart'], 'P')

            if len(pfilenames) == 0:
                print "Did not find any files. Exit."
                print pfilenames
                exit(1)

            print "%s\t%s%s%s%s" % (run_id, str(diff[-1]).ljust(20), str(fdata['metadata_stop_time']).ljust(28), str(ddata['tstop']).ljust(28), pfilenames[0])
            #print "%s\t%s%s%s" % (run_id, str(diff[-1]).ljust(20), str(fdata['metadata_start_time']).ljust(28), str(ddata['tstart']).ljust(28))

#    minBin = int(min(diff) - 10)
#    maxBin = int(max(diff) + 10)
#
#    pylab.figure()
#    pylab.hist(diff, bins = range(minBin, maxBin), range = (-50, 50))
#    pylab.xlabel('(file - i3live) start time')
#    pylab.ylabel('# of Runs')
#    pylab.axis([-200, 200, 0, 500])
#    pylab.savefig(filename)

def create_stop_hist(incompleted_runs, file_data, db_data, filename):
    diff = []

    for run_id in db_data.keys():
        if run_id in incompleted_runs:
            continue

        fdata = file_data[str(run_id)]

        if not (fdata['metadata_stop_time'] is not None and fdata['metadata_start_time'] is not None and len(fdata['missing_files']) == 0):
            continue

        ddata = db_data[int(run_id)]

        diff.append((fdata['metadata_stop_time'] - ddata['tstop']).total_seconds())

        if diff[-1] > 0 or diff[-1] < -2:
            tools = RunTools(run_id, passNumber = 2)
            pfilenames = tools.GetRunFiles(ddata['tstart'], 'P')

            if len(pfilenames) == 0:
                print "found no PFDS files. Exit."
                print pfilenames
                exit(1)

            print "%s\t%s%s%s%s" % (run_id, str(diff[-1]).ljust(20), str(fdata['metadata_stop_time']).ljust(28), str(ddata['tstop']).ljust(28), pfilenames[-1])

#    minBin = int(min(diff) - 10)
#    maxBin = int(max(diff) + 10)
#
#    pylab.figure()
#    pylab.hist(diff, bins = range(minBin, maxBin), range = (-50, 50))
#    pylab.xlabel('(file - i3live) start time')
#    pylab.ylabel('# of Runs')
#    pylab.axis([-100, 20, 0, 200])
#    pylab.savefig(filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', help = "JSON file created by GetMetadataRunStartStopTimes.py", type = str)
    args = parser.parse_args()
    
    slogger = libs.logger.DummyLogger()
    slogger.silece = True
    
    db = libs.databaseconnection.DatabaseConnection.get_connection('i3live', slogger)
    
    with open(args.data, 'r') as f:
        data = json.load(f)
        
        fix_file_data_dates(data)
        
        runs = data.keys()
        
        db_data = get_run_info_from_live(db, runs)
    
        incompleted_runs = get_incompleted_pfdst_runs('data/retro-2014.missing.list')
        incompleted_runs = incompleted_runs | get_incompleted_pfdst_runs('data/26Jan.not.processed')
    
        create_start_hist(incompleted_runs, data, db_data, "%s.png" % args.data)
        create_stop_hist(incompleted_runs, data, db_data, "%s.stop.png" % args.data)
