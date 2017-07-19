#!/usr/bin/env python
"""
Combines several checks wich have to be done after the
files are generated and updates the databases accordingly
"""


import os, sys
import subprocess as sub
import time
import datetime
import argparse

import glob

import libs.files
from libs.config import get_season_info, get_config, get_dataset_id_by_run, get_season_by_run
sys.path.append(get_config().get('DEFAULT', 'SQLClientPath'))
sys.path.append(get_config().get('DEFAULT', 'ProductionToolsPath'))
from RunTools import RunTools
from FileTools import *
from DbTools import *

from libs.files import get_tmpdir, get_logdir, MakeTarGapsTxtFile, MakeRunInfoFile, write_meta_xml_post_processing, tar_log_files, insert_gap_file_info_and_delete_files, GapsFile
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.checks import CheckFiles
import libs.process
from GoodRuntimeAdjust import main as GoodRuntimeAdjust

from libs.databaseconnection import DatabaseConnection

import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

m_live = live.MySQL()    
dbs4_ = dbs4.MySQL()   
dbs2_ = dbs2.MySQL()    

from icecube import dataio, dataclasses, icetray

def remove_empty_files(run_folder, dataset_id, run_id, logger, dryrun):
    files = glob.glob(os.path.join(run_folder, 'Level2pass2_IC*_data_Run00*_Subrun00000000_00000*_gaps.txt'))
    files = [f for f in files if not os.path.getsize(f)]

    if len(files):
        logger.warning('Found empty gaps files')
    else:
        logger.info('No files to remove')
        return True

    data_files = [f.replace('_gaps.txt', '.i3.zst') for f in files]

    for f in data_files:
        logger.info('Check {0}'.format(f))

        o = dataio.I3File(f)

        only_i_frame = True

        while o.more():
            frame = o.pop_frame()

            if frame.Stop != icetray.I3Frame.TrayInfo:
                only_i_frame = False
                break

        o.close()

        if not only_i_frame:
            logger.error('Found empty gaps file but data file contains more than just I-frames: {0}'.format(f))
            return False

        # Ok, remove files
        rfiles = glob.glob(f.replace('.i3.zst', '') + '*')

        for rf in rfiles:
            logger.info('Remove {0}'.format(rf))

            sql = """
                UPDATE i3filter.urlpath u JOIN i3filter.run r ON u.queue_id = r.queue_id SET u.transferstate = "IGNORED"
                WHERE r.dataset_id = {dataset_id} AND u.dataset_id = {dataset_id} AND r.run_id = {run_id} AND u.name LIKE '{filenames}'
            """.format(dataset_id = dataset_id, run_id = run_id, filenames = os.path.basename(f).replace('.i3.zst', '') + '%')

            logger.debug('SQL: {0}'.format(sql))

            if not dryrun:
                os.remove(rf)
                dbs4_.execute(sql)

    return True

def main_run(r, logger, dataset_id, season, nometadata, dryrun = False, no_pass2_gcd_file = False, npx = False):
    sDay = r['tStart']
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)

    run_folder = "/data/exp/IceCube/%s/filtered/level2pass2/%s%s/Run00%s_%s" % (sY, sM, sD, r['run_id'], r['production_version'])

    logger.info("======= Checking %s %s ==========="  %(str(r['run_id']),str(r['production_version'])))

    if dataset_id < 0:
        logger.error("Cannot determine the dataset id. Skipping.")
        return
    else:
        logger.info("Dataset id was determined to %s" % dataset_id)

    if DbTools(r['run_id'], dataset_id).AllOk():
        logger.warning( """Processing of Run=%s, production_version=%s
                 may not be complete ... skipping"""\
                %(r['run_id'],str(r['production_version'])))
        return    
     
    # check i/o files in data warehouse and Db
    logger.info("Checking Files in Data warehouse and database records ...")
    if CheckFiles(r, logger, dataset_id = dataset_id, season = season, dryrun = dryrun, no_pass2_gcd_file = no_pass2_gcd_file):
        logger.error("FilesCheck failed: for Run=%s, production_version=%s"\
        %(r['run_id'],str(r['production_version'])))
        return
    logger.info("File checks  .... passed")

    # Remove empty files (they are outside of the good time range
    if not remove_empty_files(run_folder, dataset_id, r['run_id'], logger, dryrun):
        return

    # Check if files cover the period good start/end time (in case a file has been forgotten etc.)
    l2files = [f for f in RunTools(r['run_id'], logger, passNumber = 2).GetRunFiles(r['tStart'], 'L') if f.endswith('_gaps.txt')]
    first_gaps = GapsFile(l2files[0], logger)
    last_gaps = GapsFile(l2files[-1], logger)

    first_gaps.read()
    last_gaps.read()

    first_event_of_first_file = first_gaps.get_first_event()
    last_event_of_last_file = last_gaps.get_last_event()

    first_event_of_first_file = dataclasses.I3Time(first_event_of_first_file['year'], first_event_of_first_file['frac']).date_time
    last_event_of_last_file = dataclasses.I3Time(last_event_of_last_file['year'], last_event_of_last_file['frac']).date_time

    logger.info('Checking good start/stop times')
    # Check if the times deviate for more than 1 second
    if (r['good_tstart'] - first_event_of_first_file).total_seconds() < -1:
        logger.error('Probably missing a file or data:')
        logger.error('  good start time:  {0}'.format(r['good_tstart']))
        logger.error('  first file start: {0}'.format(first_event_of_first_file))
        return

    if (r['good_tstop'] - last_event_of_last_file).total_seconds() > 1:
        logger.error('Probably missing a file or data:')
        logger.error('  good stop time:  {0}'.format(r['good_tstop']))
        logger.error('  last file start: {0}'.format(last_event_of_last_file))
        return

    logger.debug("--Attempting to tar _gaps.txt files ...")
    MakeTarGapsTxtFile(dbs4_, r['tStart'], r['run_id'], datasetid = dataset_id, dryrun = dryrun, logger = logger)
    logger.debug("MakeTarGapsFile              .... passed")
    logger.info( "--Attempting to collect Active Strings/DOMs information from verified GCD file ...")

    R = RunTools(r['run_id'], passNumber = 2)
    if 1 == R.GetActiveStringsAndDoms(season, UpdateDB = not dryrun):
        logger.error("GetActiveStringsAndDoms failed")
        return

    if not dryrun: dbs4_.execute("""update i3filter.grl_snapshot_info_pass2 
                         set validated=1
                         where run_id=%s and production_version=%s"""%\
                     (r['run_id'],str(r['production_version'])))

    sql = """   INSERT INTO post_processing
                    (run_id, dataset_id, validated, date_of_validation)
                VALUES
                    (%s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    validated = %s,
                    date_of_validation = NOW()
                    """ % (r['run_id'], dataset_id, 1, 1)

    logger.debug("SQL: %s" % sql)

    if not dryrun:
        filter_db = DatabaseConnection.get_connection('filter-db', logger)
        filter_db.execute(sql)

    if not nometadata:
        dest_folder = ''
        if dryrun:
            dest_folder = get_tmpdir()
        else:
            dest_folder = run_folder

        write_meta_xml_post_processing(dest_folder = dest_folder,
                                       level = 'L2',
                                       script_file = __file__,
                                       logger = logger,
                                       npx = npx)
    else:
        logger.info("No meta data files will be written")

    logger.debug('tar log files')

    tar_log_files(run_path = run_folder, dryrun = dryrun, logger = logger)

    # Write gap file info into filter-db and get rid of the gaps files
    insert_gap_file_info_and_delete_files(run_path = run_folder, dryrun = dryrun, logger = logger)

    logger.info("Checks passed")
    logger.info("======= End Checking %i %i ======== " %(r['run_id'],r['production_version'])) 
    return

def main(runinfo, logger, nometadata, dryrun = False, no_pass2_gcd_file = False, npx = False):
    datasets = set()

    for run in runinfo:
        try:
            # Get the dataset id and season for this run
            dataset_id = get_dataset_id_by_run(int(run['run_id']))
            season = get_season_by_run(run['run_id']);

            # If the dataset id is good, add it to the list of processed dataset ids
            if dataset_id > 0:
                datasets.add(dataset_id)
            
            main_run(run, logger, dataset_id = dataset_id, season = season, nometadata = nometadata, dryrun = dryrun, no_pass2_gcd_file = no_pass2_gcd_file, npx = npx)
        except Exception as e:
            logger.exception("Exception %s thrown for: Run=%s, production_version=%s" %(e.__repr__(),run['run_id'],str(run['production_version'])))
   
    if len(datasets) > 1:
        logger.warning("We have runs from more than one dataset: %s" % datasets)

    if runinfo:
        # Create run info files for all dataset ids thta are affected
        for dataset in datasets:
            MakeRunInfoFile(dbs4_, dataset_id = dataset, logger = logger, dryrun = dryrun) 


if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument('--runs', nargs="*", help="Runs to postprocess" ,type = int)
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start post processing from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End post processing at this run")
    parser.add_argument("--nometadata", action="store_true", default=False, dest="NOMETADATA", help="Don't write meta data files")
    parser.add_argument("--no-pass2-gcd-file", action="store_true", default=False, help="If there is no special pass2 GCD file for this runs or season, use this option. Then it looks at the pass1 folder for the verified GCDs.")
    parser.add_argument("--cron", action="store_true", default=False, dest="CRON", help="Use this option if you call this script via a cron")
    parser.add_argument("--npx", action="store_true", default=False, help="Use this option if you let run this script on NPX")
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(sublogpath = 'PostProcessing'), 'PostProcessing_')

    if args.CRON:
        LOGFILE = LOGFILE + 'CRON_'

    logger = get_logger(args.loglevel, LOGFILE, args.npx)

    # Check if --cron option is enabled. If so, check if cron usage allowed by config
    lock = None
    if args.CRON:
        if not get_config().getboolean('L2', 'CronPostProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = libs.process.Lock(os.path.basename(__file__), logger)
        lock.lock()

    RunInfo = None

    runs = args.runs

    if runs is None:
        runs = []

    if args.startrun is not None:
        if args.endrun is None:
            logger.critical('If --startrun, -s has been set, also the --endrun, -e needs to be set.')
            exit(1)

        runs.extend(range(args.startrun, args.endrun + 1)) 
    elif args.endrun is not None:
        logger.critical('If --endrun, -e has been set, also the --startrun, -s needs to be set.')
        exit(1)

    logger.debug('Runs: %s' % runs)

    if len(runs) > 0:
        RunInfo = dbs4_.fetchall("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info_pass2 g
                                  JOIN i3filter.run_info_summary_pass2 r ON r.run_id=g.run_id
                                  WHERE g.submitted AND (g.good_i3 OR g.good_it) AND NOT validated AND g.run_id IN (%s)
                                  ORDER BY g.run_id""" % (', '.join([str(r) for r in runs])), UseDict=True)
    else: 
        RunInfo = dbs4_.fetchall("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info_pass2 g
                                 JOIN i3filter.run_info_summary_pass2 r ON r.run_id=g.run_id
                                 WHERE g.submitted AND (g.good_i3 OR g.good_it) AND NOT validated
                                 ORDER BY g.run_id""", UseDict=True)

    logger.debug("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info_pass2 g
                                  JOIN i3filter.run_info_summary_pass2 r ON r.run_id=g.run_id
                                  WHERE g.submitted AND (g.good_i3 OR g.good_it) AND NOT validated AND g.run_id IN (%s)
                                  ORDER BY g.run_id""" % (', '.join([str(r) for r in runs])))

    logger.debug("RunInfo = %s" % str(RunInfo))

    main(RunInfo, logger, args.NOMETADATA, dryrun = args.dryrun, no_pass2_gcd_file = args.no_pass2_gcd_file, npx = args.npx)

    if args.CRON:
        lock.unlock()

    logger.info('Post processing completed')

