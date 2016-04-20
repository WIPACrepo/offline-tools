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

from RunTools import *
from FileTools import *

import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2
import SQLClient_i3live as live


m_live = live.MySQL()
dbs4_ = dbs4.MySQL()
dbs2_ = dbs2.MySQL()

from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir
from libs.runs import get_run_status as GetRunStatus
from libs.dbtools import max_queue_id as MaxQId
import libs.config
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
        

def SubmitRunL3(DDatasetId,SDatasetId,Run,QId,OUTDIR,AGGREGATE,logger,dryrun=False):
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
        if not dryrun: os.makedirs(OutDir)
    
    firstSubRun = runInfo[0]['sub_run']
    lastSubRun = runInfo[-1]['sub_run']
    
    if AGGREGATE > 1:
        groups_ = range(firstSubRun,lastSubRun,AGGREGATE)
    else:
        groups_ = range(firstSubRun,lastSubRun+1)
    
    # FIXME: g["sub_run"] == 1 is a wild guess and not kosher!
    # FIXME: we need something to find the gcd for a run    
    # This seems to be fixed, however 
    GCDEntry = [g for g in runInfo if "GCD" in g['name']][0]
    #GCDEntry = [g for g in runInfo if g['sub_run']==1 and "GCD" in g['name']][0]
    GCDFile = os.path.join(GCDEntry['path'][5:],GCDEntry['name'])
    lnCmd = "ln -sf %s %s"%(GCDFile,os.path.join(OutDir,os.path.basename(GCDFile)))
    logger.info("Linking GCDFile %s to %s" %(GCDFile,OutDir))
    if not dryrun: os.system(lnCmd)
    for g in range(len(groups_)-1):
        QId+=1
        if not dryrun:
            # entries in the job table will tell iceprod to queue the jobs
            dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"WAITING")"""%(DDatasetId,QId))
            dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
           (DDatasetId,QId,GCDEntry['name'],GCDEntry['path'],GCDEntry['md5sum'],GCDEntry['size']))
    
        p = [r for r in runInfo if r['sub_run'] in range(groups_[g],groups_[g+1])and str(r['sub_run']).zfill(8)+"_" not in r['name'] and r['type']=="PERMANENT"]
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
    
    p = [r for r in runInfo if r['sub_run'] in range(groups_[g+1],lastSubRun+1) and str(r['sub_run']).zfill(8)+"_" not in r['name'] and r['type']=="PERMANENT"]
    for q in p:
        if not dryrun: dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                         (DDatasetId,QId,q['name'],q['path'],q['md5sum'],q['size']))
        
    if not dryrun: dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(Run,DDatasetId,QId,p[0]['sub_run'],q['date']))

def main(params, outdir, logger,dryrun=False):
    
    SDatasetId = params.SDatasetId
    DDatasetId = params.DDatasetId
    START_RUN  = params.START_RUN
    END_RUN    = params.END_RUN
    OUTDIR     = outdir
    AGGREGATE  = params.AGGREGATE
    CLEAN_DW   = params.CLEAN_DW
    DryRun     = dryrun
    
    AllRuns = []
    
    MARKED_RUNS = []
    if (END_RUN) and  (START_RUN) and END_RUN >= START_RUN:
        MARKED_RUNS = range(START_RUN,END_RUN+1)
    
    AllRuns.extend(MARKED_RUNS)
    logger.info("Processing %i runs" %len(AllRuns))

    sourceInfo = dbs4_.fetchall("""select r.run_id from i3filter.job j
                                        join i3filter.run r on j.queue_id=r.queue_id
                                        where j.dataset_id=%s and r.dataset_id=%s
                                        and r.run_id between %s and %s
                                        and j.status="OK"
                                        group by r.run_id
                                        order by r.run_id,j.queue_id
                                        """%(SDatasetId,SDatasetId,START_RUN,END_RUN),UseDict=True)
    logger.info("Processing runs %s " %sourceInfo.__repr__())
    for s in sourceInfo:
        CleanRun(DDatasetId,s['run_id'],CLEAN_DW,logger,dryrun=DryRun)
        QId = MaxQId(dbs4_,DDatasetId)
        SubmitRunL3(DDatasetId,SDatasetId,s['run_id'],QId,OUTDIR,AGGREGATE,logger,dryrun=DryRun)
            
if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument("--sourcedatasetid", type=int, dest="SDatasetId", help="Dataset ID to read from, usually L2 dataset")
    parser.add_argument("--destinationdatasetid", type=int, dest="DDatasetId", help="Dataset ID to write to, usually L3 dataset")
    parser.add_argument("-s", "--startrun", type=int, default=0, dest="START_RUN", help="start submission from this run")
    parser.add_argument("-e", "--endrun", type=int, default=0,dest="END_RUN", help="end submission at this run")
    parser.add_argument("-a", "--aggregate", type=int, default=1,dest="AGGREGATE", help="number of subruns to aggregate to form one job, needed when processing 1 subrun is really short")
    parser.add_argument("-c", "--cleandatawarehouse", action="store_true", default=False,dest="CLEAN_DW", help="clean output files in datawarehouse as part of (re)submission process")
    #parser.add_option("-r", "--dryrun", action="store_true", default=False,
    #          dest="DRYRUN_", help="don't set status, just print the runs to be affected")
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(sublogpath = 'L3Processing'), 'L3Processing_')
    logger = get_logger(args.loglevel,LOGFILE)
    if not args.SDatasetId or not args.DDatasetId:
        logger.exception( "you must enter source and destination dataset_ids for submission")
        exit(1)

    if args.START_RUN and not args.END_RUN:
        logger.info( "Will only process run %i!" %args.START_RUN)
        args.END_RUN = args.START_RUN
        
    outdir_mapping = libs.config.get_var_dict('L3', 'DatasetOutputDirMapping', keytype = int)
   
    logger.debug("Output mapping: %s" % outdir_mapping);

    if args.DDatasetId not in outdir_mapping:
        logger.critical("No outdir mapped for destination dataset %s" % args.DDatasetId)
        exit(1)

    logger.debug("Selected output dir: outdir_mapping[%s] = %s" % (args.DDatasetId, outdir_mapping[args.DDatasetId]))

    main(args, outdir_mapping[args.DDatasetId], logger,dryrun=args.dryrun)
