
import sys
import argparse
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger
from dateutil.relativedelta import *
import glob

def check_run(run_id):
    db = DatabaseConnection.get_connection('dbs4', DummyLogger())

    result = db.fetchall("SELECT * FROM i3filter.run_info_summary WHERE run_id = %s" % run_id, UseDict = True)

    if len(result):
        sDay = result[0]['tStart']      # run start date
        sY = sDay.year
        sM = str(sDay.month).zfill(2)
        sD = str(sDay.day).zfill(2)
    
        first = glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*" % (sY, sM, sD))
    
        nextDate = sDay + relativedelta(days = 1)
    
        second = glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*" % (nextDate.year, str(nextDate.month).zfill(2), str(nextDate.day).zfill(2)))

        return len(first) and len(second)

    else:
        print "No run info for %s" % run_id
        return False

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--run", nargs = '+', type = int, required = True, help = "Run number(s) to process")

    args = parser.parse_args()

    ok_run_list = []

    for run_id in args.run:
        answer = check_run(run_id)

        if answer:
            ok_run_list.append(str(run_id))
            answer = 'OK'
        else:
            answer = 'BAD'

        print "Check run %s: %s" % (run_id, answer)


    print "OK runs: %s" % ' '.join(ok_run_list)
