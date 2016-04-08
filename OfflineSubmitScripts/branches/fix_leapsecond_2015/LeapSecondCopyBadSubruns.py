
"""
Creates a new run directory 'Run<RUN ID>_bad_subrun_leap_second' and copies the first
and last subrun of the current run into this directory.
"""

import os
import sys
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir, get_tmpdir, GetGoodSubruns
import libs.times
from icecube import icetray, dataio, dataclasses
from I3Tray import *

import gzip
import shutil
import glob

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
import RunTools

sys.path.append("/data/user/i3filter/SQLServers_n_Clients/")
import SQLClient_dbs4 as dbs4

dbs4_ = dbs4.MySQL()

def read_runs_from_file(file):
    runs = []

    with open(file, 'r') as f:
        runs = f.readlines()
        runs = map(lambda s: s.strip(), runs)
        runs = [r for r in runs if len(r)]

    return runs

if __name__ == "__main__":
    parser = get_defaultparser(__doc__,dryrun=True)

    parser.add_argument("-s", "--startrun", type=int, required = True, default=-1,
                      dest="STARTRUN", help="Start fixing GCD files from this run")
    
    parser.add_argument("-e", "--endrun", type=int, required = True, default=-1,
                      dest="ENDRUN", help="End fixing GCD files at this run")
    
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(), 'LeapSecondCopyBadSubruns_')
    logger = get_logger(args.loglevel,LOGFILE)

    dryrun = args.dryrun

    if args.STARTRUN > args.ENDRUN:
        logger.critical("Strart run %s must be smaller or equal to endrun %s"%(args.STARTRUN, args.ENDRUN))
        exit(1)

    sql = """SELECT snapshot_id, production_version, run_id, good_tstart, good_tstart_frac, good_tstop, good_tstop_frac
            FROM grl_snapshot_info 
            WHERE run_id BETWEEN %s AND %s
            AND (good_i3 = 1 OR good_it = 1)"""%(args.STARTRUN, args.ENDRUN)

    info = dbs4_.fetchall(sql, UseDict = True)
  
    aff_last_subruns = read_runs_from_file('LeapSecondLastSubRunAffectedRuns.txt')
    logger.info("%s runs are affected by leap second for the last subrun"%len(aff_last_subruns))

    aff_first_subruns = read_runs_from_file('LeapSecondFirstSubRunAffectedRuns.txt')
    logger.info("%s runs are affected by leap second for the first subrun"%len(aff_first_subruns))
 
    logger.debug("First subrun affected runs: %s"%str(aff_first_subruns)) 
    logger.debug("Last subrun affected runs: %s"%str(aff_last_subruns)) 
 
    for run in info:
        if str(run['run_id']) not in aff_last_subruns and str(run['run_id']) not in aff_first_subruns:
            logger.info("Skip run %s since it is not affected"%run['run_id'])
            continue

        logger.info('=========================================')
        logger.info("Copy bad subruns for run %s"%run['run_id'])

        date = run['good_tstart']

        # Run base dir
        basedir = "/data/exp/IceCube/%s/filtered/level2/%s%s"%(str(date.year), str(date.month).zfill(2), str(date.day).zfill(2))
        
        rundir = os.path.join(basedir, "Run%s"%str(run['run_id']).zfill(8))
        bad_subrun_dir = rundir + '_bad_subrun_leap_second' 

        logger.info("Run directory is %s"%rundir)
        logger.info("Bad sub run directory is %s"%bad_subrun_dir)

        # Needs to be executed before the new folder is created
        all_run_files = RunTools.RunTools(run['run_id']).GetRunFiles(date, 'L')

        try:
            if not dryrun:
                os.mkdir(bad_subrun_dir)
                logger.info('Created directory')
        except OSError as e:
            logger.error('Could not make directory')
            logger.exception(e)
            continue

        # find last subrun
        if str(run['run_id']) in aff_last_subruns:
            logger.info("Last subrun is affected")
            if not len(all_run_files):
                logger.error("No file sfound for run %s"%run['run_id'])

            last_subrun_path = [file for file in all_run_files if '_IT.i3.bz2' in file][-1][:-10] + '*'
            
            logger.info("Searching for last sub run: %s"%last_subrun_path)

            last_subrun = glob.glob(last_subrun_path)
            logger.info("Found %s files for last subrun"%(len(last_subrun)))

            for file in last_subrun:
                dest = os.path.join(bad_subrun_dir, os.path.basename(file))

                logger.info("Copy %s"%(file))
                logger.info("  to %s"%(dest))
                
                if not dryrun:
                    shutil.copyfile(file, dest)

        # New folder is created. Lets copy files
        if str(run['run_id']) in aff_first_subruns:
            logger.info("First subrun is affected")
            first_subrun_path = [file for file in all_run_files if '_IT.i3.bz2' in file][0][:-10] + '*'
            
            logger.info("Searching for first sub run: %s"%first_subrun_path)

            first_subrun = glob.glob(first_subrun_path)
            logger.info("Found %s files for first subrun"%(len(first_subrun)))

            for file in first_subrun:
                dest = os.path.join(bad_subrun_dir, os.path.basename(file))

                logger.info("Copy %s"%(file))
                logger.info("  to %s"%(dest))
                
                if not dryrun:
                    shutil.copyfile(file, dest)


