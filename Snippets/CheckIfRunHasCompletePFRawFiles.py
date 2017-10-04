
import sys
import argparse
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger
from dateutil.relativedelta import *
import glob
import datetime

from get_sub_run_info import get_sub_run_info

from icecube import dataio, dataclasses

def check_run(run_id):
    db = DatabaseConnection.get_connection('filter-db', DummyLogger())

    result = db.fetchall("SELECT * FROM i3filter.runs WHERE run_id = %s" % run_id, UseDict = True)

    if len(result):
        sDay = result[0]['tstart']      # run start date
        sY = sDay.year
        sM = str(sDay.month).zfill(2)
        sD = str(sDay.day).zfill(2)
    
        files = glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*%s*.tar.gz" % (sY, sM, sD, run_id))
    
        nextDate = sDay + relativedelta(days = 1)
    
        files.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*%s*.tar.gz" % (nextDate.year, str(nextDate.month).zfill(2), str(nextDate.day).zfill(2), run_id)))

        files.sort()

        files = [{'file': f, 'id': int(f.split('Subrun00000000_')[1].split('.')[0])} for f in files]
        ids = [f['id'] for f in files]

        max_file_id = max(ids)
        min_file_id = min(ids)

        if min_file_id != 0:
            print 'Missing file 0'
            return False

        if len(files) != max_file_id + 1:
            print 'Missing files:'

            # It's dave to iterate until max_file_id - 1 since max_file_id has been found...
            for i in range(max_file_id):
                if i not in ids:
                    print i

            return False

        # check end date
        pass1 = get_sub_run_info(run_id, db, True)

        try:
            pass1_last_time = pass1[run_id][pass1[run_id].keys()[-1]]['last_event'].date_time
        except KeyError:
            pass1_last_time = datetime.datetime.min

        file_time = enddate_of_file(files[-1]['file'])
        if abs((file_time - result[0]['tstop']).total_seconds()) > 1 and abs((file_time - pass1_last_time).total_seconds()) > 1:
            print 'Missing more than one second at end of run: %s seconds' % (file_time - result[0]['tstop']).total_seconds()
            print 'File time: %s' % file_time
            print 'tstop:     %s' % result[0]['tstop']
            print 'pass1:     %s' % pass1_last_time
            return False

        return True
    else:
        print "No run info for %s" % run_id
        return False

def enddate_of_file(f):
    i3f = dataio.I3File(f)

    latest_time = None

    while i3f.more():
        frame = i3f.pop_frame()
        if 'I3EventHeader' in frame:
            latest_time = frame['I3EventHeader'].end_time.date_time

    i3f.close()

    return latest_time

def season(season):
    if season == None:
        return []

    from CompareGRLs import read_file

    grl = None
    skip_first_lines = -1

    if season == 2010:
        grl = '/data/exp/IceCube/2010/filtered/level2/IC79_GRLists/IC79_GRL_NewFormat.txt'
        skip_first_lines = 1
    else:
        grl = '/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo.txt' % (season, season)
        skip_first_lines = 2

    return read_file(grl).keys()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--run", nargs = '+', type = int, required = True, help = "Run number(s) to process")
    parser.add_argument("--season", type = int, default = None, required = False, help = "Season")
    parser.add_argument("--datasets", nargs = '+', type = int, default = None, required = False, help = "Filter submitted runs: datasets")

    args = parser.parse_args()

    ok_run_list = []

    submitted = []

    if args.datasets is not None:
        db = DatabaseConnection.get_connection('dbs4', DummyLogger())
        query = db.fetchall('SELECT DISTINCT run_id FROM i3filter.run WHERE dataset_id IN ({})'.format(','.join([str(d) for d in args.datasets])), UseDict = True)

        submitted = [r['run_id'] for r in query]

    args.run.extend(season(args.season))
    args.run.sort()

    print 'Run candidates: {all_runs}\tchecking: {checking}\tsubmitted: {submitted}'.format(all_runs = len(args.run), checking = len(args.run) - len(submitted), submitted = len(submitted))

    for run_id in args.run:
        if run_id in submitted:
            print 'Skip run {} because it has alreadby been submitted'.format(run_id)
            continue

        answer = check_run(run_id)

        if answer:
            ok_run_list.append(run_id)
            answer = 'OK'
        else:
            answer = 'BAD'

        print "Check run %s: %s" % (run_id, answer)


    print "BAD runs {1}: {0}".format(' '.join([str(e) for e in list(set(args.run) - set(ok_run_list))]), len(list(set(args.run) - set(ok_run_list))))
    print "OK runs {1}: {0}".format(' '.join([str(r) for r in ok_run_list]), len(ok_run_list))
