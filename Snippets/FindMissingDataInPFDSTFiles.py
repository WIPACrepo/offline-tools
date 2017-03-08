
import sys
from databaseconnection import DatabaseConnection
import argparse
from CompareGRLs import read_file
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
from libs.logger import DummyLogger

def get_SDST_info(runs):
    db = DatabaseConnection.get_connection('sdst', DummyLogger())

    sql = "SELECT Run, Subrun, StartEv, EndEv FROM SDSTFile WHERE Run IN (%s) ORDER BY Run" % ','.join([str(r) for r in runs])

    rawdata = db.fetchall(sql, UseDict = True)

    result = {}

    for row in rawdata:
        if row['Run'] not in result:
            result[row['Run']] = {}

        result[row['Run']][row['Subrun']] = row

    return result

def find_bad_files(run_id, run, hide_missing_subrun = False):
    last_subrun = max(run.keys())
    for sub_run, data in run.iteritems():
        if sub_run == last_subrun:
            continue

        if sub_run + 1 not in run.keys():
            if not hide_missing_subrun:
                print "Found problem in run %s: sub run %s not found" % (run_id, sub_run + 1)
        elif data['EndEv'] + 1 != run[sub_run + 1]['StartEv']:
            print "Found problem in run %s: Subrun %s -> %s (EndEv %s + 1 != StartEv %s)" % (run_id, sub_run, sub_run + 1, data['EndEv'], run[sub_run + 1]['StartEv'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--grl', required = True, help = "Good run list", type = str)
    parser.add_argument('--hide-missing-subrun', help = "Hide missing subrun messages", action = "store_true", default = False)
    args = parser.parse_args()

    # Keys are run numbers
    grl = read_file(args.grl)
    print "Good run list loaded"

    data = get_SDST_info(grl.keys())
    #data = get_SDST_info([120177])

    print "SDST file info loaded"

    runs = sorted(data.keys())

    for run_id in runs:
        value = data[run_id]

        find_bad_files(run_id, value, args.hide_missing_subrun)
    
