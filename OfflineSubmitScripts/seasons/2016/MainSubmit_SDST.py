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
from libs.files import get_logdir, get_tmpdir, get_existing_check_sums, write_meta_xml_main_processing, remove_path_prefix
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
    sql = "SELECT run_id, good_it, good_i3 FROM i3filter.runs WHERE run_id = %s ORDER BY snapshot_id DESC LIMIT 1" % run_id

    result = db.fetchall(sql, UseDict = True)

    logger.debug(result)

    if len(result):
        return int(result[0]['good_it']) and int(result[0]['good_i3'])
    else:
        return False

def submit_run(checksumcache, db, gcd, run_id, status, DatasetId, QueueId, dryrun, add, logger):
    # Using grid or NPX?
    grid = db.fetchall("SELECT * FROM i3filter.grid_statistics WHERE dataset_id = %s;" % DatasetId, UseDict = True)

    logger.debug("DB result = %s" % grid)

    path_prefix = 'gsiftp://gridftp.icecube.wisc.edu'

    InFiles = []
  
    fdb = DatabaseConnection.get_connection('filter-db', logger)
 
    run_info = fdb.fetchall("SELECT * FROM i3filter.runs WHERE run_id = %s ORDER BY snapshot_id DESC LIMIT 1" % run_id, UseDict = True)

    #run_info = db.fetchall("SELECT * FROM i3filter.grl_snapshot_info g JOIN i3filter.run_info_summary s ON g.run_id = s.run_id WHERE g.run_id = %s ORDER BY snapshot_id DESC LIMIT 1" % run_id, UseDict = True)
 
    logger.debug("run_info = %s" % run_info)

    g = run_info[0]

    sDay = g['tstart']      # run start date
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)
    
    logger.debug('Get PFFilt files')

    InFiles = glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*PFRaw_*_Run00%s_Subrun00000000_00000*.tar.gz" % (sY, sM, sD, run_id))
    InFiles.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*PFRaw_*_Run00%s_Subrun00000000_00000*.tar.zst" % (sY, sM, sD, run_id)))

    nextDate = sDay + relativedelta(days = 1)

    InFiles.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*PFRaw_*_Run00%s_Subrun00000000_00000*.tar.gz" % (nextDate.year, str(nextDate.month).zfill(2), str(nextDate.day).zfill(2), run_id)))       
    InFiles.extend(glob.glob("/data/exp/IceCube/%s/unbiased/PFRaw/%s%s/*PFRaw_*_Run00%s_Subrun00000000_00000*.tar.zst" % (nextDate.year, str(nextDate.month).zfill(2), str(nextDate.day).zfill(2), run_id)))       
   
    InFiles.sort()

    logger.debug("InFiles = %s" % InFiles)

    # Check for "recovered-data_*" files. If such files are present, remove same file w/o prefix
    recovered_data_files = [f for f in InFiles if os.path.basename(f).startswith('recovered-data_')]

    for rf in recovered_data_files:
        orig_name = os.path.join(os.path.dirname(rf), os.path.basename(rf).split('recovered-data_')[1])
        if orig_name in InFiles:
            InFiles.remove(orig_name)
            logger.warning('Found a file with recovered data: {}'.format(rf))
            logger.warning('Also the original file has been found: {}'.format(orig_name))
            logger.warning('The original file will be ignored.')

    logger.info("Found %s PFRaw files for this run" % len(InFiles))

    if add:
        sql = '''SELECT 
    path, name
FROM
    i3filter.run r
        JOIN
    i3filter.urlpath u ON r.dataset_id = u.dataset_id
        AND r.queue_id = u.queue_id
WHERE
    r.dataset_id = {dataset_id} AND run_id = {run_id}
        AND `type` = 'INPUT'
        AND name NOT LIKE '%GCD%'
ORDER BY name'''.format(dataset_id = DatasetId, run_id = run_id)

        submitted_files = db.fetchall(sql, UseDict = True)

        submitted_files = [os.path.join(remove_path_prefix(f['path']), f['name']) for f in submitted_files]

        InFiles = [f for f in InFiles if f not in submitted_files]

        logger.warning('The --add option has been activated. Only {} files will be submitted in addition to the already submitted ones.'.format(len(InFiles)))

        logger.debug('Already submitted files:')
        for f in submitted_files:
            logger.debug('  ' + f)

    season = get_season_by_run(run_id)

    logger.debug('Season = %s' % season)

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

    if gcd is None:
        if season == 2010:
            gcd_glob_str = "/data/exp/IceCube/%s/filtered/level2a/%s%s/Level2a_IC79_data_Run00%s_GCD.i3.bz2" % (sY, sM, sD, g['run_id'])

            logger.debug('gcd_glob_str = %s' % gcd_glob_str)

            GCDFileName = glob.glob(gcd_glob_str)
        else:
            GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/VerifiedGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))

        if not len(GCDFileName):
            GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/AllGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
    else:
        GCDFileName.append(gcd)
 
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
        exit(1)
    
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
    filter_db = DatabaseConnection.get_connection('filter-db', logger)

    submitted_runs = get_submitted_runs(args.datasetid, db, logger)
    checksumcache = DBChecksumCache(logger, args.dryrun)

    if args.gcd is not None and len(args.run) != 1:
        logger.critical('You defined a certain GCD file. If you do this you are allowed to submit exactly one run. {} runs found.'.format(len(args.run)))
        exit(1)

    for run_id in args.run:
        logger.info("********* Run %s *********" % run_id)

        if not is_good_run(run_id, filter_db, logger) and not args.force_submission:
            logger.error("Run %s is not a good run. Run will be skipped." % run_id)
            continue
        elif args.force_submission:
            logger.warning('Run %s is marked as bad or no good run information is available. This run has been forced to get submitted.' % run_id)

        if run_id in submitted_runs:
            if args.add:
                logger.info("Try to add some files to run %s" % run_id)
            elif args.resubmission:
                logger.info("Run %s will be resubmitted" % run_id)
            else:
                logger.info("Run %s will be skipped" % run_id)
                continue
        else:
            logger.info("Run %s will be submitted" % run_id)

        if not args.add:
            clean_run(db, args.datasetid, run_id, args.cleandatawarehouse, None, logger, args.dryrun, ignore_production_version = True)
        else:
            logger.info('Do not clean the DB since we want to add some files...')
        
        qId = max_queue_id(db, args.datasetid)
            
        submit_run(checksumcache, db, args.gcd, run_id, 'WAITING', args.datasetid, qId, args.dryrun, args.add, logger)

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)

    parser.add_argument("--datasetid", type = int, required = True, help = "The dataset id")
    parser.add_argument("--gcd", type = str, required = False, default = None, help = "Specify a GCD file. Use this option if this script does not automatically find the (correct) GCD file. Only allowed if just one run is submitted")

    parser.add_argument("--run", nargs = '+', type = int, required = True, help = "Run number(s) to process")

    parser.add_argument("--cleandatawarehouse", action = "store_true", default = False, help = "Clean output files in datawarehouse as part of (re)submission process.")
    parser.add_argument("--force-submission", action = "store_true", default = False, help = "Ignore if the run is marked as bad (or if no good run information is available).")
    parser.add_argument("--add", action = "store_true", default = False, help = "Use this option if already some files ahve been processed but accidentally some files weren't submitted. It submits only the missing files.")

    parser.add_argument("--resubmission", action = "store_true", default = False,
              help = "If a run has already been submitted, resubmit it. If this option is not set, alrady submitted runs are skipped.")

    args = parser.parse_args()

    logfile = os.path.join(get_logdir(), 'SDST_%s_' % args.datasetid)

    logger = get_logger(args.loglevel, logfile)

    main(args, logger)
