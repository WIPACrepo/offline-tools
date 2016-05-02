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

from RunTools import RunTools
from FileTools import *
from DbTools import *

from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir,MakeTarGapsTxtFile,MakeRunInfoFile
from libs.checks import CheckFiles
from libs.config import get_season_info, get_config, get_dataset_id_by_run
from GoodRuntimeAdjust import main as GoodRuntimeAdjust

import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

m_live = live.MySQL()    
dbs4_ = dbs4.MySQL()   
dbs2_ = dbs2.MySQL()    

def main_run(r, logger, dataset_id, dryrun = False):
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
    if CheckFiles(r,logger,dryrun=dryrun):
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
    R.GetActiveStringsAndDoms(2015,UpdateDB=True)
    if not dryrun: dbs4_.execute("""update i3filter.grl_snapshot_info 
                         set validated=1
                         where run_id=%s and production_version=%s"""%\
                     (r['run_id'],str(r['production_version'])))           
    logger.info("======= End Checking %i %i ======== " %(r['run_id'],r['production_version'])) 
    return

def main(runinfo,logger,dryrun=False):
    datasets = []

    for run in runinfo:
        try:
            # Get the dataset id for this run
            dataset_id = get_dataset_id_by_run(int(run['run_id']))

            # If the dataset id is good, add it to the list of processed dataset ids
            if dataset_id > 0:
                datasets.append(dataset_id)
            
            main_run(run, logger, dataset_id = dataset_id, dryrun = dryrun) 
        except Exception as e:
            logger.exception("Exception %s thrown for: Run=%s, production_version=%s" %(e.__repr__(),run['run_id'],str(run['production_version'])))
   
    if len(datasets) > 1:
        logger.warning("We have runs from more than one dataset: %s" % datasets)

    if runinfo:
        # Create run info files for all dataset ids thta are affected
        for dataset in datasets:
            MakeRunInfoFile(dbs4_, dataset_id = dataset, dryrun = dryrun) 


if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument('-r',nargs="?", help="run to postprocess",dest="run",type=int)
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(sublogpath = 'PostProcessing'), 'PostProcessing_')
    logger = get_logger(args.loglevel, LOGFILE)

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

    main(RunInfo, logger, dryrun = args.dryrun)
