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

import libs.files
from libs.config import get_season_info, get_config, get_dataset_id_by_run, get_season_by_run
sys.path.append(get_config().get('DEFAULT', 'SQLClientPath'))
sys.path.append(get_config().get('DEFAULT', 'ProductionToolsPath'))
from RunTools import RunTools
from FileTools import *
from DbTools import *

from libs.files import get_tmpdir, get_logdir, MakeTarGapsTxtFile, MakeRunInfoFile, write_meta_xml_post_processing
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.checks import CheckFiles
import libs.process
from GoodRuntimeAdjust import main as GoodRuntimeAdjust

import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

m_live = live.MySQL()    
dbs4_ = dbs4.MySQL()   
dbs2_ = dbs2.MySQL()    

def main_run(r, logger, dataset_id, season, nometadata, dryrun = False):
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
    if CheckFiles(r, logger, dataset_id = dataset_id, season = season, dryrun = dryrun):
        logger.error("FilesCheck failed: for Run=%s, production_version=%s"\
        %(r['run_id'],str(r['production_version'])))
        return
    logger.info("File checks  .... passed")

    ## delete/trim files when Good start/stop differ from Run start/stop
    logger.info( "--Attempting to make adjustments to output Files to ensure all events fall within GoodRun start/stop time ...")
    GoodRuntimeAdjust(r['run_id'], r['production_version'], dataset_id = dataset_id, logger = logger, dryrun = dryrun)
    logger.debug( "GoodRunTimeAdjust   .... passed")

    logger.debug("--Attempting to tar _gaps.txt files ...")
    MakeTarGapsTxtFile(dbs4_, r['tStart'], r['run_id'], datasetid = dataset_id, dryrun = dryrun, logger = logger)
    logger.debug("MakeTarGapsFile              .... passed")
    logger.info( "--Attempting to collect Active Strings/DOMs information from verified GCD file ...")

    R = RunTools(r['run_id'])
    if 1 == R.GetActiveStringsAndDoms(season, UpdateDB = not dryrun):
        logger.error("GetActiveStringsAndDoms failed")
        return

    if not dryrun: dbs4_.execute("""update i3filter.grl_snapshot_info 
                         set validated=1
                         where run_id=%s and production_version=%s"""%\
                     (r['run_id'],str(r['production_version'])))

    if not nometadata:
        dest_folder = ''
        if dryrun:
            dest_folder = get_tmpdir()
        else:
            sDay = r['tStart']
            sY = sDay.year
            sM = str(sDay.month).zfill(2)
            sD = str(sDay.day).zfill(2)

            dest_folder = "/data/exp/IceCube/%s/filtered/level2/%s%s/Run00%s_%s" % (sY, sM, sD, r['run_id'], r['production_version'])

        write_meta_xml_post_processing(dest_folder = dest_folder,
                                       level = 'L2',
                                       script_file = __file__,
                                       logger = logger)
    else:
        logger.info("No meta data files will be written")

    logger.info("Checks passed")
    logger.info("======= End Checking %i %i ======== " %(r['run_id'],r['production_version'])) 
    return

def main(runinfo, logger, nometadata, dryrun = False):
    datasets = set()

    for run in runinfo:
        try:
            # Get the dataset id and season for this run
            dataset_id = get_dataset_id_by_run(int(run['run_id']))
            season = get_season_by_run(run['run_id']);

            # If the dataset id is good, add it to the list of processed dataset ids
            if dataset_id > 0:
                datasets.add(dataset_id)
            
            main_run(run, logger, dataset_id = dataset_id, season = season, nometadata = nometadata, dryrun = dryrun) 
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
    parser.add_argument('-r',nargs="?", help="run to postprocess",dest="run",type=int)
    parser.add_argument("--nometadata", action="store_true", default=False, dest="NOMETADATA", help="Don't write meta data files")
    parser.add_argument("--cron", action="store_true", default=False, dest="CRON", help="Use this option if you call this script via a cron")
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(sublogpath = 'PostProcessing'), 'PostProcessing_')

    if args.CRON:
        LOGFILE = LOGFILE + 'CRON_'

    logger = get_logger(args.loglevel, LOGFILE)

    # Check if --cron option is enabled. If so, check if cron usage allowed by config
    lock = None
    if args.CRON:
        if not get_config().getboolean('L2', 'CronPostProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = libs.process.Lock(os.path.basename(__file__), logger)

    season = get_config().getint('DEFAULT', 'Season')
    test_runs = get_season_info(season)['test']

    # Workaround for empty lists (the query needs to have at least one element)
    if len(test_runs) == 0:
        test_runs = [-1]

    if args.run is not None:
        RunInfo = dbs4_.fetchall("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info g
                                  JOIN i3filter.run_info_summary r ON r.run_id=g.run_id
                                  WHERE g.submitted AND (g.good_i3 OR g.good_it or g.run_id IN (%s)) AND NOT validated AND g.run_id = %i
                                  ORDER BY g.run_id""" % (','.join([str(r) for r in test_runs]), args.run), UseDict=True)
    else: 
        RunInfo = dbs4_.fetchall("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info g
                                 JOIN i3filter.run_info_summary r ON r.run_id=g.run_id
                                 WHERE g.submitted AND (g.good_i3 OR g.good_it OR g.run_id IN (%s)) AND NOT validated
                                 ORDER BY g.run_id""" % ','.join([str(r) for r in test_runs]), UseDict=True)

    main(RunInfo, logger, args.NOMETADATA, dryrun = args.dryrun)

    if args.CRON:
        lock.unlock()

    logger.info('Post processing completed')

