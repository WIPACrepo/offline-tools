#!/usr/bin/env python

"""
Creates entries in 'url_path', 'jobs', and 'run' tables in dbs4
  to be used by IceProd in creating/submitting L3 jobs
"""


import sys, os
from os.path import join, exists
import glob
import time
import datetime
import pymysql as MySQLdb

from libs.files import get_logdir, get_tmpdir, write_meta_xml_main_processing
import libs.config
sys.path.append(libs.config.get_config().get('DEFAULT', 'SQLClientPath'))
sys.path.append(libs.config.get_config().get('DEFAULT', 'ProductionToolsPath'))

from RunTools import *
from FileTools import *

import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2
import SQLClient_i3live as live


m_live = live.MySQL()
dbs4_ = dbs4.MySQL()
dbs2_ = dbs2.MySQL()

from libs.logger import get_logger, delete_log_file
from libs.argparser import get_defaultparser
from libs.runs import get_run_status as GetRunStatus
from libs.runs import set_post_processing_state, get_validated_runs_L2
from libs.dbtools import max_queue_id as MaxQId
import libs.process
import json

# FIXME: this CleanRun is slightly different from libs.runs.clean_ru
# TODO: unify!

def CleanRun(DatasetId,Run,CLEAN_DW,logger,dryrun=False):
    """
    Purge Run information from dbs4 and clean datawarehouse (optional)
    
    Args:
        DatasetId (int): dataset id
        Run (int): run number to purge
        CLEAN_DW (bool): remove files from datawarehouse
        logger (loggin.Logger): the loggin instance

    Keyword Args:
        dryrun (bool): don't do actual work
    
    Returns: 
        None
    """    
    tmp  = dbs4_.fetchall(""" select j.queue_id from i3filter.job j
                          join i3filter.run r on j.queue_id=r.queue_id
                          where r.dataset_id=%s and j.dataset_id=%s
                          and r.run_id=%s"""\
                          %(DatasetId,DatasetId,Run) )
    if not len(tmp):
        logger.warning("No database entries for run %i, unable to remove this run!" %Run)
        return 
    
    CleanListStr = ",".join([str(t[0]) for t in tmp])
    if CLEAN_DW:
        # clean only output files, exclude INPUT = {PFFilt, GCD} files
        tmp1 = dbs4_.fetchall(""" SELECT path,name FROM i3filter.urlpath
                                                 where dataset_id=%s and queue_id in (%s) and type!="INPUT" """%(DatasetId,CleanListStr))
    
        if len(tmp1):
            for t in tmp1:
                filename = t[0][5:]+"/"+t[1]
                if os.path.isfile(filename):
                    logger.debug("deleting %s " %filename)
                    if not dryrun: os.system("rm %s"%filename)
    if not dryrun:                 
        dbs4_.execute("""delete from i3filter.job where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))
        dbs4_.execute("""delete from i3filter.urlpath where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))
        dbs4_.execute("""delete from i3filter.run where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))
        set_post_processing_state(run_id = Run, dataset_id = DatasetId, validated = 0, dbs4 = dbs4_, dryrun = dryrun, logger = logger)
        

def SubmitRunL3(DDatasetId, SDatasetId, Run, QId, OUTDIR, AGGREGATE, logger, linkonlygcd, nometadata, dryrun = False, cosmicray = False):
    """
    Submit a run for Level3 processing

    Args:
        DDatasetId (int): the dataset id of the L3 
        SDatasetId (int): the dataset id of the L2
        Run (int): run to process
        QId (int): the maximum queue_id of this run
        OUTDIR (str): the output directory for the processed files
        AGGREGATE (int): combine AGGREGATE L2 files to a single L3 file
        logger (logging.Logger): the logging instance to use

    Keyword Args:
        dryrun (bool): don't do actual work
        
    Returns:
        None
    """
    assert AGGREGATE > 0
        
    runInfo = dbs4_.fetchall("""select r.date,r.sub_run,u.* from i3filter.job j
                                join i3filter.run r on r.queue_id=j.queue_id
                                join i3filter.urlpath u on u.queue_id=j.queue_id
                                where j.dataset_id=%s and r.dataset_id=%s and u.dataset_id=%s
                                and (u.type="PERMANENT" or name like "%%GCD%%" ) and r.run_id=%s and j.status !="BadRun"
                                order by r.sub_run
                                """%(SDatasetId,SDatasetId,SDatasetId,str(Run)),UseDict=True)


    if not len(runInfo):
        logger.exception("No L2 files for this run %s!"%str(Run))
        exit(1)
        
    date_ = runInfo[0]['date']
    date_ = str(date_.year)+ "/"+str(date_.month).zfill(2)+str(date_.day).zfill(2)
    OutDir = os.path.join(OUTDIR,date_,"Run00"+str(Run))
    if not os.path.exists(OutDir):
        if not dryrun or linkonlygcd:
            os.makedirs(OutDir)
            logger.debug("Makedirs: %s" % OutDir)
    
    firstSubRun = runInfo[0]['sub_run']
    lastSubRun = runInfo[-1]['sub_run']
    
    if AGGREGATE > 1:
        groups_ = range(firstSubRun,lastSubRun,AGGREGATE)
    else:
        groups_ = range(firstSubRun,lastSubRun+1)
    
    # FIXME: g["sub_run"] == 1 is a wild guess and not kosher!
    # FIXME: we need something to find the gcd for a run    
    # This seems to be fixed, however 

    if not cosmicray:
        GCDEntry = [g for g in runInfo if "GCD" in g['name']][0]
        #GCDEntry = [g for g in runInfo if g['sub_run']==1 and "GCD" in g['name']][0]
        GCDFile = os.path.join(GCDEntry['path'][5:],GCDEntry['name'])
    else:
        season = libs.config.get_season_by_run(Run)

        if season == -1:
            logger.critical("Could not determine season for run %s" % Run)
            exit(1)

        GCDFile = glob.glob("/data/ana/CosmicRay/IceTop_level3/exp/*/GCD/Level3_*%s_data_Run00%s_*_GCD.i3.gz" % (season, Run))

        if len(GCDFile) != 1:
            logger.debug("glob = %s" % ("/data/ana/CosmicRay/IceTop_level3/exp/*/GCD/Level3_*%s_data_Run00%s_*_GCD.i3.gz" % (season, Run)))
            logger.critical("Found more than one GCD file: %s" % GCDFile)
            exit(1)

        GCDFile = GCDFile[0]

        GCDEntry = {
            'name': os.path.basename(GCDFile),
            'path': "gsiftp://gridftp.icecube.wisc.edu%s/" % os.path.dirname(GCDFile),
            'md5sum': FileTools(GCDFile).md5sum(),
            'size': os.path.getsize(GCDFile)
        }

        logger.debug("GCDFile = %s" % GCDFile)
        logger.debug("GCDEntry = %s" % GCDEntry)

    lnCmd = "ln -sf %s %s"%(GCDFile,os.path.join(OutDir,os.path.basename(GCDFile)))
    logger.info("Linking GCDFile %s to %s" %(GCDFile,OutDir))

    if not dryrun or linkonlygcd:
        os.system(lnCmd)
        logger.debug("Created GCD link")

    for g in range(len(groups_)-1):
        QId+=1

        p = None
        if not cosmicray:
            p = [r for r in runInfo if r['sub_run'] in range(groups_[g],groups_[g+1]) and str(r['sub_run']).zfill(8)+"_" not in r['name'] and r['type']=="PERMANENT"]
        else:
            p = [r for r in runInfo if r['sub_run'] in range(groups_[g],groups_[g+1]) and str(r['sub_run']).zfill(8)+"_IT" in r['name'] and r['type']=="PERMANENT"]

        logger.debug("p = %s" % p)

        # be aware of gaps in the L2 files
        if not len(p):
            continue

        if not dryrun:
            # entries in the job table will tell iceprod to queue the jobs
            dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"WAITING")"""%(DDatasetId,QId))
            dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
           (DDatasetId,QId,GCDEntry['name'],GCDEntry['path'],GCDEntry['md5sum'],GCDEntry['size']))
    
        # p are the subruns in the aggregated batch 
        for q in p:
            if not dryrun:dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""%(DDatasetId,QId,q['name'],q['path'],q['md5sum'],q['size']))
        if not dryrun: dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(Run,DDatasetId,QId,p[0]['sub_run'],q['date']))
    
    QId+=1
    # entries in job, urlpath and run tables
    if not dryrun:
        dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"WAITING")"""%(DDatasetId,QId))
        dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
       (DDatasetId,QId,GCDEntry['name'],GCDEntry['path'],GCDEntry['md5sum'],GCDEntry['size']))
    
    p = None
    if not cosmicray:
        p = [r for r in runInfo if r['sub_run'] in range(groups_[g+1],lastSubRun+1) and str(r['sub_run']).zfill(8)+"_" not in r['name'] and r['type']=="PERMANENT"]
    else:
        p = [r for r in runInfo if r['sub_run'] in range(groups_[g+1],lastSubRun+1) and str(r['sub_run']).zfill(8)+"_IT" in r['name'] and r['type']=="PERMANENT"]

    for q in p:
        logger.debug("q = %s" % q)

        if not dryrun: dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                         (DDatasetId,QId,q['name'],q['path'],q['md5sum'],q['size']))
        
    if not dryrun: dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(Run,DDatasetId,QId,p[0]['sub_run'],q['date']))

    if not nometadata:
        meta_file_dest = ''
        if dryrun:
            meta_file_dest = get_tmpdir()
        else:
            meta_file_dest = OutDir

        run_times = RunTools(Run, logger).GetRunTimes()

        write_meta_xml_main_processing(dest_folder = meta_file_dest,
                                       dataset_id = DDatasetId,
                                       run_id = Run,
                                       level = 'L3',
                                       run_start_time = run_times['tStart'],
                                       run_end_time = run_times['tStop'],
                                       logger = logger)
    else:
        logger.info("No meta data files will be written")

def main(SDatasetId, DDatasetId, START_RUN, END_RUN, AGGREGATE, CLEAN_DW, outdir, LINK_ONLY_GCD, NOMETADATA, RESUBMISSION, IGNORE_L2_VALIDATION, logger, DryRun, cosmicray = False):
    validatedRuns = get_validated_runs_L2(dbs4_, START_RUN, END_RUN)
 
    logger.info("Processing runs between %s and %s" % (START_RUN, END_RUN))

    sourceInfo = dbs4_.fetchall("""select r.run_id from i3filter.job j
                                        join i3filter.run r on j.queue_id=r.queue_id
                                        where j.dataset_id=%s and r.dataset_id=%s
                                        and r.run_id between %s and %s
                                        and j.status="OK"
                                        group by r.run_id
                                        order by r.run_id,j.queue_id
                                        """%(SDatasetId,SDatasetId,START_RUN,END_RUN),UseDict=True)

    destinationInfo = dbs4_.fetchall("""select r.run_id from i3filter.job j
                                        join i3filter.run r on j.queue_id=r.queue_id
                                        where j.dataset_id=%s and r.dataset_id=%s
                                        and r.run_id between %s and %s
                                        group by r.run_id
                                        order by r.run_id,j.queue_id
                                        """%(DDatasetId,DDatasetId,START_RUN,END_RUN),UseDict=True)

    submittedRuns = [r['run_id'] for r in destinationInfo]

    logger.debug("source info: %s" % len(sourceInfo))

    counter = {'all': 0, 'submitted': 0, 'skipped': 0, 're': 0}
    for s in sourceInfo:
        counter['all'] = counter['all'] + 1

        if not IGNORE_L2_VALIDATION and s['run_id'] not in validatedRuns:
            logger.info("Skipping run %s because L2 is not validated yet" % s['run_id'])
            counter['skipped'] = counter['skipped'] + 1
            continue

        if s['run_id'] in submittedRuns and not LINK_ONLY_GCD:
            if not args.RESUBMISSION:
                logger.debug("Skipping run %s because it has been already submitted" % s['run_id'])
                counter['skipped'] = counter['skipped'] + 1
                continue
            else:
                logger.info("Re-submitting run %s" % s['run_id'])
                counter['re'] = counter['re'] + 1

        CleanRun(DDatasetId,s['run_id'],CLEAN_DW,logger,dryrun=DryRun)
        QId = MaxQId(dbs4_,DDatasetId)
        try:
            SubmitRunL3(DDatasetId, SDatasetId, s['run_id'], QId, outdir, AGGREGATE, logger, LINK_ONLY_GCD, nometadata = NOMETADATA, dryrun=DryRun, cosmicray = cosmicray)
        except TypeError as e:
            print e
            counter['skipped'] = counter['skipped'] + 1
            continue

        counter['submitted'] = counter['submitted'] + 1

    logger.info("%s runs were handled | submitted %s runs | re-submitted %s runs | skipped %s runs" % (counter['all'], counter['submitted'], counter['re'], counter['skipped']))

    return counter
            
if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument("--sourcedatasetid", type=int, default = None, dest="SDatasetId", help="Dataset ID to read from, usually L2 dataset")
    parser.add_argument("--destinationdatasetid", type=int, required = None, dest="DDatasetId", help="Dataset ID to write to, usually L3 dataset")
    parser.add_argument("-s", "--startrun", type=int, default=0, dest="START_RUN", help="start submission from this run")
    parser.add_argument("-e", "--endrun", type=int, default=0,dest="END_RUN", help="end submission at this run")
    parser.add_argument("-a", "--aggregate", type=int, default=1,dest="AGGREGATE", help="number of subruns to aggregate to form one job, needed when processing 1 subrun is really short")
    parser.add_argument("-c", "--cleandatawarehouse", action="store_true", default=False,dest="CLEAN_DW", help="clean output files in datawarehouse as part of (re)submission process")
    parser.add_argument("--linkonlygcd", action="store_true", default=False, dest="LINK_ONLY_GCD", help="No jobs will be submitted but the GCD file(s) will be linked. Useful if some links are missing")
    parser.add_argument("--nometadata", action="store_true", default=False, dest="NOMETADATA", help="Don't write meta data files")
    parser.add_argument("--resubmission", action="store_true", default=False, dest="RESUBMISSION", help="Don't skip already submitted runs and re-submit them")
    parser.add_argument("--cron", action="store_true", default=False, dest="CRON", help="Execute as cron")
    parser.add_argument("--cosmicray", action="store_true", default=False, help="Important if you submit L3 jobs for the cosmic ray WG")
    parser.add_argument("--ignoreL2validation", action="store_true", default=False, dest="IGNORE_L2_VALIDATION", help="If you do not care if L2 has not been validated yet. ONLY USE THIS OPTION IF YOU KNOW WHAT YOU ARE DOING! Not available with --cron")
    args = parser.parse_args()

    # Check of only GCDs should be linked
    # If yes, act like an dryrun except for the linking
    if args.LINK_ONLY_GCD:
        args.dryrun = True

    LOGFILE=os.path.join(get_logdir(sublogpath = 'L3Processing'), 'L3Processing_')

    if args.CRON:
        LOGFILE = LOGFILE + 'CRON_'

    logger = get_logger(args.loglevel,LOGFILE)

    if (args.SDatasetId is None or args.DDatasetId is None) and not args.CRON:
        logger.critical("--sourcedatasetid and --destinationdatasetid are required")
        exit(1)

    lock = None
    if args.CRON:
        if not libs.config.get_config().getboolean('L3', 'CronMainSubmission'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = libs.process.Lock(os.path.basename(__file__), logger)
        lock.lock()

    logger.debug("Dryrun: %s" % args.dryrun)
    logger.debug("Create only GCD links: %s" % args.LINK_ONLY_GCD)

    if args.START_RUN and not args.END_RUN:
        logger.info( "Will only process run %i!" %args.START_RUN)
        args.END_RUN = args.START_RUN
        
    outdir_mapping = libs.config.get_var_dict('L3', 'DatasetOutputDirMapping', keytype = int)
   
    logger.warning("****** You have activated the Cosmic Ray WG option! *******")

    logger.debug("Output mapping: %s" % outdir_mapping);

    if not args.CRON:
        if args.DDatasetId not in outdir_mapping:
            logger.critical("No outdir mapped for destination dataset %s" % args.DDatasetId)
            exit(1)

        logger.debug("Selected output dir: outdir_mapping[%s] = %s" % (args.DDatasetId, outdir_mapping[args.DDatasetId]))

        main(SDatasetId = args.SDatasetId,
            DDatasetId = args.DDatasetId, 
            START_RUN = args.START_RUN, 
            END_RUN = args.END_RUN, 
            AGGREGATE = args.AGGREGATE, 
            CLEAN_DW = args.CLEAN_DW, 
            LINK_ONLY_GCD = args.LINK_ONLY_GCD, 
            NOMETADATA = args.NOMETADATA, 
            outdir = outdir_mapping[args.DDatasetId], 
            RESUBMISSION = args.RESUBMISSION,
            IGNORE_L2_VALIDATION = args.IGNORE_L2_VALIDATION,
            logger = logger, 
            DryRun = args.dryrun,
            cosmicray = args.cosmicray)
    else:
        # Find installed crons
        crons = libs.config.get_var_dict('L3', 'CronJobMainProcessing', keytype = int, valtype = int)

        firstrun = libs.config.get_config().get('L3', 'CronRunStart')
        lastrun = libs.config.get_config().get('L3', 'CronRunEnd')

        delete_log = True

        for dest, source in crons.iteritems():
            if dest not in outdir_mapping:
                logger.error("Cron `CronJobMainProcessing%s = %s` cannot be executed since no output dir for %s is mapped." % (dest, source, dest))
                continue

            logger.info('====================================================')
            logger.info("Executing Cron Job for dataset %s with source %s" % (dest, source))

            logger.debug("Selected output dir: outdir_mapping[%s] = %s" % (dest, outdir_mapping[dest]))

            counter = main(SDatasetId = source,
                DDatasetId = dest, 
                START_RUN = firstrun, 
                END_RUN = lastrun, 
                AGGREGATE = args.AGGREGATE, 
                CLEAN_DW = False, # never resubmit or clean something within a cron 
                LINK_ONLY_GCD = False, # Doesn't make sense for a cron to turn it to `True`
                NOMETADATA = False,
                outdir = outdir_mapping[dest], 
                RESUBMISSION = False, # never resubmit or clean something within a cron
                IGNORE_L2_VALIDATION = False, # not available for crons
                logger = logger, 
                DryRun = args.dryrun)

            # counter contains how many runs have been submitted. Since
            # the cron is executed very often and will probably do nothing
            # we won't keep those log files since they are useless.
            # Therefore, we will delete the log file if no run has been submitted
            if counter['submitted']:
                delete_log = False

        if delete_log:
            delete_log_file(logger)

    if args.CRON:
        lock.unlock()

