
import sys
import argparse
from databaseconnection import DatabaseConnection
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
from libs.logger import DummyLogger

def show_gcd_info():
    filter_db = DatabaseConnection.get_connection('filter-db', DummyLogger())
    dbs4 = DatabaseConnection.get_connection('dbs4', DummyLogger())

    runs = filter_db.fetchall("SELECT * FROM i3filter.runs WHERE (good_i3 OR good_it) AND run_id > 129500", UseDict = True)
    submitted = [str(s['run_id']) for s in dbs4.fetchall("SELECT DISTINCT run_id FROM i3filter.run WHERE run_id > 129500", UseDict = True)]

    runs = sorted(runs, key = lambda run: run['run_id'])

    result = []
    for run in runs:
        if str(run['run_id']) not in submitted:
            result.append(run)

    print "{run_id:>10} | {snapshot_id:>10} | {good_i3:>3} | {good_it:>3} | {gcd_generated:>9} | {gcd_bad_doms_validated:>9} | {gcd_pole_validation:>10} | {gcd_template_validation:>10} | {good_tstart:>21} | {tstart:>21} ".format(run_id = 'Run Id', snapshot_id = 'Snapshot', good_i3 = 'I3', good_it = 'IT', good_tstart = 'good_tstart', tstart = 'tstart', gcd_generated = 'GCD gen', gcd_bad_doms_validated = 'GCD BDL', gcd_pole_validation = 'GCD Pole', gcd_template_validation = 'GCD Temp')
    for run in result:
        run['tstart'] = str(run['tstart'])
        run['good_tstart'] = str(run['good_tstart'])
        print  "{run_id:>10} | {snapshot_id:>10} | {good_i3:>3} | {good_it:>3} | {gcd_generated:>9} | {gcd_bad_doms_validated:>9} | {gcd_pole_validation:>10} | {gcd_template_validation:>10} | {good_tstart:>21} | {tstart:>21} ".format(**run)

parser = argparse.ArgumentParser()
parser.add_argument('--gcd-info', action = 'store_true', default = False, help = "Show GCD generation info")
args = parser.parse_args()

if args.gcd_info:
    show_gcd_info()


