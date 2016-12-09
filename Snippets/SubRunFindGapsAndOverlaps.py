
import sys
import argparse
from datetime import datetime
import math

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
import libs.databaseconnection
import libs.logger
from icecube import dataclasses

parser = argparse.ArgumentParser()
parser.add_argument('--run', nargs = "*", help = "Runs that should be checked", type = int)
parser.add_argument('--grlruns', nargs = "*", help = "Use GRL runs instead of a specific run", type = str)
args = parser.parse_args()

# Check parameters
if args.run is None and args.grlruns is None:
    print "Specify runs!"
    exit()

if args.grlruns:
    from CompareGRLs import read_file as read_grl
    args.run = []

    for file in args.grlruns:
        args.run.extend(read_grl(file).keys())

    args.run = [int(r) for r in args.run]

    print "Read %s GRL(s) that contain %s runs" % (len(args.grlruns), len(args.run))

exclude = (116193, 116219, 120087)

for r in exclude:
    if r in args.run:
        args.run.remove(r)

filter_db = libs.databaseconnection.DatabaseConnection.get_connection('filter-db', libs.logger.DummyLogger())

def get_subrun_info(run_id, filter_db):
    info = filter_db.fetchall('SELECT * FROM i3filter.sub_runs WHERE run_id = %s ORDER BY run_id, sub_run' % run_id, UseDict = True)

    sub_runs = {}

    for row in info:
        start_time = dataclasses.I3Time(row['first_event_year'], row['first_event_frac'])
        stop_time = dataclasses.I3Time(row['last_event_year'], row['last_event_frac'])

        sub_runs[row['sub_run']] = (start_time, stop_time, float(row['livetime']))

    return sub_runs

def get_livetime(run_ids):
    dbs4 = libs.databaseconnection.DatabaseConnection.get_connection('i3live', libs.logger.DummyLogger())

    sql = """SELECT 
    runNumber AS `run_id`,
    snapshot_id,
    IFNULL(good_tstart, tstart) AS `good_tstart`,
    IFNULL(good_tstop, tstop) AS `good_tstop`,
    TIME_TO_SEC(TIMEDIFF(IFNULL(good_tstop, tstop),
                    IFNULL(good_tstart, tstart))) AS `livetime`
FROM
    live.livedata_snapshotrun s
        JOIN
    live.livedata_run r ON r.id = s.run_id
WHERE runNumber IN (%s) ORDER BY runNumber, snapshot_id""" % (','.join([str(r) for r in run_ids]))

    #sql = "SELECT run_id, snapshot_id, good_tstart, good_tstop, TIME_TO_SEC(TIMEDIFF(good_tstop, good_tstart)) AS `livetime` FROM grl_snapshot_info WHERE run_id IN (%s) ORDER BY run_id, snapshot_id" % (','.join([str(r) for r in run_ids]))

    run_infos = dbs4.fetchall(sql, UseDict = True)

    livetimes = {}

    for run in run_infos:
        livetimes[run['run_id']] = (run['good_tstart'], run['good_tstop'], run['livetime'])
#        livetimes[run['run_id']] = (datetime.strptime(run['good_tstart'], '%Y-%m-%d %H:%M:%S'), datetime.strptime(run['good_tstop'], '%Y-%m-%d %H:%M:%S'), run['livetime'])

    return livetimes

def get_gaps(run_ids, filter_db):
    info = filter_db.fetchall("SELECT * FROM gaps WHERE run_id IN (%s)" % ','.join([str(r) for r in run_ids]), UseDict = True)

    gaps = {}
    for row in info:
        if row['run_id'] not in gaps:
            gaps[row['run_id']] = []

        gaps[row['run_id']].append(row)

    return gaps

def check_livetime_with_gaps(run_info, subrun_livetime, gaps):
    gaps_total = sum([float(sr['delta_time']) for sr in gaps])

    if math.fabs(run_info[2] - gaps_total - subrun_livetime) > 1:
        print "\t---- Gaps are not enough... ---------------"
        print "\tGaps Livetime: %s" % subrun_livetime
        print "\tGaps total   : %s" % gaps_total
        print "\tGRL Livetime : %s" % run_info[2]
        print "\tDifference   : %s" % (run_info[2] - subrun_livetime)
        return False

    return True

def check_overlap(sub_run_info):
    subruns = sub_run_info.keys()
    subruns.sort()

    overlap = False

    for i in range(len(subruns) - 1):
        current_sr = sub_run_info[subruns[i]]
        next_sr = sub_run_info[subruns[i + 1]]

        if current_sr[1] >  next_sr[0]:
            print "\t---- Overlap found: %s -> %s---------------" % (subruns[i], subruns[i + 1])
            print "\tEnd time of SR %s:\t%s" % (subruns[i], current_sr[1])
            print "\tStart time of SR %s:\t%s" % (subruns[i + 1], next_sr[0])
            print "\tDifference:       \t%s" % (current_sr[1].date_time - next_sr[0].date_time).seconds

            overlap = True

    return overlap

def check_first_and_last_event(run_info, sub_run_info):
    subruns = sub_run_info.keys()

    res = [False, False]

    if sub_run_info[subruns[0]][0].date_time.second != run_info[0].second or sub_run_info[subruns[-1]][1].date_time.second != run_info[1].second:
        print "\t---- Run Start/Stop, Sub Run Start/Stop mismatch found ---------------------"
        if sub_run_info[subruns[0]][0].date_time.second != run_info[0].second:
            print "\tGood Start Time         : %s" % run_info[0]
            print "\tFirst Sub Run Start Time: %s" % sub_run_info[subruns[0]][0]
            print "\tDifference (GST - FSRSR): %s" % (run_info[0] - sub_run_info[subruns[0]][0].date_time).seconds

            res[0] = True

        if sub_run_info[subruns[-1]][1].date_time.second != run_info[1].second:
            print "\tGood Stop Time          : %s" % run_info[1]
            print "\tLast Sub Run Stop Time  : %s" % sub_run_info[subruns[-1]][1]
            print "\tDifference (LSRSR - GST): %s" % (sub_run_info[subruns[-1]][1].date_time - run_info[1]).seconds

            res[1] = True

    return res

def check_for_gaps_between_sub_runs(sub_run_info, min_gap_size):
    sub_runs = sub_run_info.keys()
    sub_runs.sort()
    
    gaps = []

    for i in range(len(sub_runs) - 1):
        current_sr = sub_run_info[sub_runs[i]]
        next_sr = sub_run_info[sub_runs[i + 1]]

        diff = (next_sr[0].date_time - current_sr[1].date_time).seconds

        if math.fabs(diff) >= min_gap_size:
            print "\t---- Found gap between sub runs -------------------"
            print "\tSub runs: %s -> %s" % (sub_runs[i], sub_runs[i + 1])
            print "\tGap size: %s" % diff

            gaps.append({'sub_run': sub_runs[i], 'next_sub_run': sub_runs[i + 1], 'diff': diff})

    return gaps

def check_run(run_id, run_info, sub_run_info, gaps, gaps_between_srs):
    gaps_livetime = sum([i[2] for k, i in sub_run_info.iteritems()])

    if not (gaps_livetime > run_info[2] + 1) and not (gaps_livetime < run_info[2] + 1): # +1 because GRL livetime only second precise
        return None

    print "===== Found deviation for run %s =========================" % run_id
    print "Gaps livetime : %s" % gaps_livetime
    print "GRL livetime  : %s" % run_info[2]
    print "GRL start time: %s" % run_info[0]
    print "GRL stop time : %s" % run_info[1]
    print "-------------------------"
    print "Difference    : %s" % (gaps_livetime - run_info[2])
    print ""

    overlap = check_overlap(sub_run_info)
    flsr = check_first_and_last_event(run_info, sub_run_info)

    lt_w_gaps = check_livetime_with_gaps(run_info, gaps_livetime, gaps)

    gaps_between_sub_runs = check_for_gaps_between_sub_runs(sub_run_info, 1)

    gaps_between_srs[run_id] = gaps_between_sub_runs

    print ""

    return [overlap, flsr[0], flsr[1], len(gaps_between_sub_runs) > 0, gaps_livetime, run_info[2], lt_w_gaps, run_info[0], run_info[1], (gaps_livetime - run_info[2])]

print "Get Run Livetimes"

run_info = get_livetime(args.run)
sub_run_info = {}

summary = {}

print "Get Sub Run Info"
for run in args.run:
    sub_run_info[run] = get_subrun_info(run, filter_db)

print "Get Gaps"
gaps = get_gaps(args.run, filter_db)

gaps_between_srs = {}

print "Start Checks"
for run in args.run:
    g = []

    if run in gaps:
        g = gaps[run]

    result = check_run(run,
                        run_info[run], 
                        sub_run_info[run], 
                        g, 
                        gaps_between_srs)

    if result is not None:
        summary[run] = result

print ""

print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
print "++++ Summary +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

# sort by difference type
run_ids = summary.keys()
tmp_run_ids = []
for r in run_ids:
    if summary[r][9] < 0 or len(tmp_run_ids) == 0:
        tmp_run_ids.append(r)
    else:
        tmp_run_ids.insert(0, r)

run_ids = tmp_run_ids

print "-------------------------------------------------------------------------------------------------------------------------------------------------------------"
print "Run ID\tSub Run Overlap?\tFirst SR Wrong\tLast SR Wrong\tGaps btw SRs\tGaps LT\t\tGRL LT\tGRL LT Match w/ gaps\tGRL ST\t\t\tGRL ST\t\t\tDifference"
print "--------------------------------------------------------------------------------------------------------------------------------------------------"
for run in run_ids:
    val = summary[run]

    print "%s\t%s\t\t\t%s\t\t%s\t\t%s\t\t%s%s\t%s\t\t\t%s\t%s\t%s" % (run, val[0], val[1], val[2], val[3], str(val[4]).ljust(16, ' '), val[5], val[6], val[7], val[8], val[9])

print ""

print "Gaps between sub runs: %s/%s" % (sum([int(v[3]) for k, v in summary.iteritems()]), len(args.run))
print "Detailed view"

print "Run ID\tGap Location\tGap Size in Seconds"
print "-------------------------------------------"

for run_id, info in gaps_between_srs.iteritems():
    first = True
    
    for sr in info:
        r = '      '
        if first:
            r = run_id
            first = False

        print "%s\t%s -> %s\t%s" % (r, str(sr['sub_run']).rjust(3), str(sr['next_sub_run']).rjust(3), str(sr['diff']).rjust(3))
