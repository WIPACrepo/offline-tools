#!/usr/bin/env python

"""
Creates PFDST entries in 'url_path', 'jobs', and 'run' tables in dbs4
to be used by IceProd in creating/submitting SDST jobs.
"""

import sys, os
from os.path import expandvars, join, exists
import glob
import time
import datetime
from dateutil.relativedelta import *

from RunTools import *
from FileTools import *
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir, get_tmpdir, get_existing_check_sums, write_meta_xml_main_processing
from libs.checks import runs_already_submitted
from libs.runs import get_run_status, clean_run
from libs.dbtools import max_queue_id 
from libs.config import get_dataset_id_by_run
from libs.config import get_season_by_run
from libs.databaseconnection import DatabaseConnection
from libs.utils import DBChecksumCache


##-----------------------------------------------------------------
## setup DB
##-----------------------------------------------------------------

def get_submitted_runs(dataset_id, db, logger):
    submitted_runs = []

    sql = "SELECT DISTINCT run_id FROM i3filter.run WHERE dataset_id = %s" % dataset_id
    result = db.fetchall(sql, UseDict = True)

    for row in result:
        submitted_runs.append(int(row['run_id']))

    return list(set(submitted_runs))

def is_good_run(run_id, db, logger):
    sql = "SELECT run_id, good_it, good_i3 FROM i3filter.grl_snapshot_info WHERE run_id = %s ORDER BY snapshot_id DESC LIMIT 1" % run_id

    result = db.fetchall(sql, UseDict = True)

    logger.debug(result)

    if len(result):
        return int(result[0]['good_it']) and int(result[0]['good_i3'])
    else:
        return False

def submit_run(checksumcache, db, run_id, status, DatasetId, QueueId, dryrun, logger):
    # Using grid or NPX?
    grid = db.fetchall("SELECT * FROM i3filter.grid_statistics WHERE dataset_id = %s;" % DatasetId, UseDict = True)

    logger.debug("DB result = %s" % grid)

    path_prefix = 'gsiftp://gridftp.icecube.wisc.edu'

    InFiles = []
   
    run_info = db.fetchall("SELECT * FROM i3filter.grl_snapshot_info g JOIN i3filter.run_info_summary s ON g.run_id = s.run_id WHERE g.run_id = %s ORDER BY snapshot_id DESC LIMIT 1" % run_id, UseDict = True)
 
    logger.debug("run_info = %s" % run_info)

    g = run_info[0]

    sDay = g['tStart']      # run start date
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)
    
    logger.debug('Get PFFilt files')

    InFiles = glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*PFRaw_PhysicsFiltering_Run00%s_Subrun00000000_00000*.tar.gz" % (sY, sM, sD, run_id))

    nextDate = sDay + relativedelta(days = 1)

    InFiles.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*PFRaw_PhysicsFiltering_Run00%s_Subrun00000000_00000*.tar.gz" % (nextDate.year, str(nextDate.month).zfill(2), str(nextDate.day).zfill(2), run_id)))       
   
    InFiles.sort()

    logger.debug("InFiles = %s" % InFiles)

    logger.info("Found %s PFRaw files for this run" % len(InFiles))

    season = get_season_by_run(run_id)

    dst_folder = 'PFDST'
    if season in (2015, 2016):
        dst_folder += 'noSPE'

    MainOutputDir = OutputDir = "/data/exp/IceCube/%s/unbiased/%s/%s%s/"%(sY, dst_folder, sM, sD)
    if not os.path.exists(MainOutputDir) and not dryrun:
        os.makedirs(MainOutputDir)
    
    if not os.path.exists(OutputDir) and not dryrun:
        os.makedirs(OutputDir)
    
    logger.debug('OutputDir = %s' % OutputDir)

    logger.debug('Find GCD file')

    GCDFileName = []
    GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/VerifiedGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))

    if not len(GCDFileName):
        GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/AllGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
    
    logger.debug("GCD files = %s" % GCDFileName)
    
    if len(GCDFileName):
        logger.debug('Calculate MD5 sum and create symlink for GCD file')

        GCDFileName = GCDFileName[0]
        GCDFileChkSum = str(FileTools(GCDFileName, logger).md5sum())
        
        lnGCDFile = os.path.join(OutputDir,os.path.basename(GCDFileName))
       
        logger.debug("ln -sf %s %s"%(GCDFileName, lnGCDFile))
 
        if not dryrun:
            lnCmd = "ln -sf %s %s"%(GCDFileName, lnGCDFile)
            os.system(lnCmd)
    else:
        GCDFileName = ""
        logger.critical("No GCD file found.")
    
    if not len(InFiles):
        logger.info("No PFRaw will be submitted for run %s"%g['run_id'])
    
        QueueId+=1
    
        if not dryrun:
            db.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
            db.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,-1,str(sDay.date())))
    else:
        logger.info("Attempting to submit %s PFRaw Files for run %s"%(str(len(InFiles)),g['run_id']))
    
        for InFile in InFiles:
            CountSubRun = int(InFile[-15:-7])
            
            logger.info("Submission of file %s / %s -> %s" % (CountSubRun, len(InFiles), InFile))

            QueueId+=1
    
            if not dryrun:
                db.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
    
            if os.path.isfile(GCDFileName) and not dryrun:
                db.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DatasetId,QueueId,os.path.basename(GCDFileName),path_prefix+os.path.dirname(GCDFileName)+"/",GCDFileChkSum,str(os.path.getsize(GCDFileName))))
    
            InFileChkSum = checksumcache.get_md5(InFile)
    
            if not dryrun:
                db.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DatasetId,QueueId,os.path.basename(InFile),path_prefix+os.path.dirname(InFile)+"/",InFileChkSum,str(os.path.getsize(InFile))))
    
                db.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,CountSubRun,str(sDay.date())))

def main(args, logger):
    db = DatabaseConnection.get_connection('dbs4', logger)

    submitted_runs = get_submitted_runs(args.datasetid, db, logger)
    checksumcache = DBChecksumCache(logger, args.dryrun)

    for run_id in args.run:
        logger.info("********* Run %s *********" % run_id)

        if not is_good_run(run_id, db, logger):
            logger.error("Run %s is not a good run. Run will be skipped." % run_id)
            continue

        if run_id in submitted_runs:
            if args.resubmission:
                logger.info("Run %s will be resubmitted" % run_id)
            else:
                logger.info("Run %s will be skipped" % run_id)
                continue
        else:
            logger.info("Run %s will be submitted" % run_id)

        
        clean_run(db, args.datasetid, run_id, args.cleandatawarehouse, None, logger, args.dryrun, ignore_production_version = True)
        
        qId = max_queue_id(db, args.datasetid)
            
        submit_run(checksumcache, db, run_id, 'WAITING', args.datasetid, qId, args.dryrun, logger)

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)

    parser.add_argument("--datasetid", type = int, required = True, help = "The dataset id")


    parser.add_argument("--run", nargs = '+', type = int, required = True, help = "Run number(s) to process")

    parser.add_argument("--cleandatawarehouse", action = "store_true", default = False, help = "Clean output files in datawarehouse as part of (re)submission process.")

    parser.add_argument("--resubmission", action = "store_true", default = False,
              help = "If a run has already been submitted, resubmit it. If this option is not set, alrady submitted runs are skipped.")

    args = parser.parse_args()

    logfile = os.path.join(get_logdir(), 'SDST_%s_' % args.datasetid)

    logger = get_logger(args.loglevel, logfile)

    main(args, logger)
