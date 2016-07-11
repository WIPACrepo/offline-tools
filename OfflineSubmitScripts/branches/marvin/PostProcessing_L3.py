#!/usr/bin/env python


#############################################################################
#
#  General Description: does post-production checks for L3 sets 
#
#
# Copyright: (C) 2014 The IceCube collaboration
#
# @file    $Id$
# @version $Revision$

# @date    12/04/2014
# @author  Oladipo Fadiran <ofadiran@icecube.wisc.edu>
#
#############################################################################

import sys, os
from os.path import expandvars, join, exists
import glob
from optparse import OptionParser
import time
import datetime
import pymysql as MySQLdb
import cPickle
import datetime
from dateutil.relativedelta import *
import subprocess as sub

import traceback

from RunTools import *
from FileTools import *

from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir
from libs.runs import set_post_processing_state, get_validated_runs
import libs.config

##-----------------------------------------------------------------
## setup DB
##-----------------------------------------------------------------

import SQLClient_i3live as live
m_live = live.MySQL()

import SQLClient_dbs4 as dbs4
dbs4_ = dbs4.MySQL()

import SQLClient_dbs2 as dbs2
dbs2_ = dbs2.MySQL()

def main(args, logger):
    SDatasetId = args.SDATASETID
    DDatasetId = args.DDATASETID
    START_RUN = args.STARTRUN
    END_RUN = args.ENDRUN
    SEASON = libs.config.get_season_by_run(START_RUN)
    MERGEHDF5 = args.MERGEHDF5

    season_of_end_run = libs.config.get_season_by_run(END_RUN)

    if SEASON != season_of_end_run:
        logger.warning("The first run (%s) is of season %s, the last run (%s) is of season %s. Only runs of season %s will be post processed." % (
            START_RUN, SEASON, END_RUN, season_of_end_run
        ))

    sourceRunInfo = dbs4_.fetchall("""SELECT r.run_id FROM i3filter.job j
                                        JOIN i3filter.run r ON j.queue_id=r.queue_id
                                        WHERE j.dataset_id=%s AND r.dataset_id=%s
                                        AND r.run_id BETWEEN %s AND %s
                                        AND j.status="OK"
                                        GROUP BY r.run_id
                                        ORDER BY r.run_id
                                        """%(SDatasetId,SDatasetId,START_RUN,END_RUN),UseDict=True)
    
    GRL = "/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo.txt"%(SEASON,SEASON)
    if not os.path.isfile(GRL):
        logger.critical("Can't access GRL file %s for run validation, check path, exiting ...... " % GRL)
        exit(1)
        
    with open(GRL,"r") as G:
        L = G.readlines()
        GoodRuns = [int(l.split()[0]) for l in L if l.split()[0].isdigit()]

    # Get validated runs that we don't validate it another time
    validated_runs = {}
    for run in get_validated_runs(DDatasetId, dbs4_, True, logger):
        validated_runs[int(run['run_id'])] = run

    for s in sourceRunInfo:
        try:    
            verified = 1
            
            RunId = s['run_id']
            
            if not RunId in GoodRuns:
                logger.info("Skip run %s since it is not in the good run list" % RunId)
                continue

            if int(RunId) in validated_runs:
                logger.debug("Run %s was already validated on %s" % (RunId, validated_runs[int(RunId)]['date_of_validation']))
                continue
            
            logger.info("Verifying processing for run %s..." % RunId)
        
            sRunInfo = dbs4_.fetchall("""SELECT * FROM i3filter.run r join i3filter.urlpath u on r.queue_id=u.queue_id
                                       join i3filter.job j on j.queue_id=u.queue_id 
                                         where j.dataset_id=%s and r.dataset_id=%s and u.dataset_id=%s and r.run_id=%s and j.status="OK"
                                         order by r.sub_run
                                              """%(SDatasetId,SDatasetId,SDatasetId,RunId),UseDict=True)
            
            dRunInfo = dbs4_.fetchall("""SELECT * FROM i3filter.run r join i3filter.urlpath u on r.queue_id=u.queue_id
                                      join i3filter.job j on j.queue_id=u.queue_id
                                         where j.dataset_id=%s and r.dataset_id=%s and u.dataset_id=%s and r.run_id=%s
                                         order by r.sub_run
                                              """%(DDatasetId,DDatasetId,DDatasetId,RunId),UseDict=True)

            # Check GCD file in L3 out dir.
            gInfo = [s for s in sRunInfo if "GCD" in s['name']] # GCD file from L2 source
            gInfo = gInfo[0]
            
            # get GCD file linked from L3 dir.
            linkedGCD = []
            linkedGCD = [s for s in dRunInfo if "Level3" in s['name']]
            linkedGCD = glob.glob(linkedGCD[0]['path'].split("file:")[1]+"/*GCD*")
            if not len(linkedGCD):
                verified = 0
                logger.error("no GCD file linked from out dir. for run %s" % RunId)
            elif not os.path.isfile(linkedGCD[0]):
                verified = 0
                logger.error("Listed GCD file in DB not in output dir. for run %s" % RunId)
            elif gInfo['md5sum']!=FileTools(linkedGCD[0], logger).md5sum():
                verified = 0
                logger.error("GCD file linked from L3 dir. has different md5sum from source L2 dir. for run %s. Source: %s, linked: %s" % (RunId, gInfo, linkedGCD[0]))
            else:
                logger.info("GCD check passed")
            # End: GCD check
            
            sRunInfo = [s for s in sRunInfo if "EHE" not in s['name'] and "_IT" not in s['name'] and "SLOP" not in s['name'] and "i3.bz2" in s['name']]
            sRunInfo_sorted = sorted(sRunInfo, key=lambda k:['name'])
            
            for sr in sRunInfo:
                nName = sr['name'].replace("Level2_","Level3_").replace("Test_","")
                nRecord = []
                nRecord = [d for d in dRunInfo if d['name']==nName]

                if len(nRecord)!=1:
                    # may just be a subrun that is good in L2 but bad in L3 e.g. really small L2 output so no L3 events
                    badRun = [d for d in dRunInfo if d['name']==sr['name']] # if no L3 output, check for L2 input record   
                    if badRun[0]['status'] == "BadRun":
                        logger.info("Skipped sub run %s since it is declared as bad in L2" % sr['sub_run'])
                        continue           # skip subrun that has been declared bad

                    verified = 0
                    logger.error("no DB record (or more than 1) for in/output %s/%s dir. for run %s" % (sr['name'], nName, RunId))
                    continue

                nRecord = nRecord[0]
                OutDir = nRecord['path']
                if nRecord['status'] not in ("OK","BadRun"):
                    verified = 0
                    logger.error("DB record for in/output %s/%s dir. for run %s is %s" % (sr['name'], nName, RunId, nRecord['status']))
                    continue

                L3Out = os.path.join(nRecord['path'][5:],nRecord['name'])
                if not os.path.isfile(L3Out):
                    verified = 0
                    logger.error("out L3 file %s does not exist in outdir. for run %s"%(L3Out,RunId))

            if verified:
                logger.info("Sub run check passed")
            
            # in case last subrun was a badrun, pick last good subrun as nRecord
            nRecord = [d for d in dRunInfo if "Level3" in d['name'] and d['status']=="OK"]            
            nRecord = nRecord[-1]

            if MERGEHDF5:
                logger.info("Merge hdf5 files")
                hdf5Files = []
                
                # ensures only files from "OK" jobs are included in the Merged file
                hInfo = dbs4_.fetchall("""SELECT * FROM i3filter.job j join i3filter.urlpath u on j.queue_id=u.queue_id
                                              join i3filter.run r on r.queue_id=j.queue_id
                                              where j.dataset_id=%s and u.dataset_id=%s and
                                              r.dataset_id=%s and j.status="OK" and r.run_id=%s
                                              and u.name like "%%hdf5%%"
                                              """%(DDatasetId,DDatasetId,DDatasetId,RunId),UseDict=True)
                
                hdf5Files = [h['path'][5:]+"/"+h['name'] for h in hInfo if "Merged" not in h['name']] # avoid previously meged hdf5 file if one exists

                if len(hdf5Files):
                    hdf5Files.sort()
                    hdf5Files = " ".join(hdf5Files)
                    
                    hdf5Out = nRecord['path'][5:]+"/Level3_IC86.%s_data_Run00%s_Merged.hdf5"%(SEASON,RunId)

                    if not args.dryrun:
                        buildir = libs.config.get_config().get('L3', "I3_BUILD_%s" % DDatasetId)
                        envpath = os.path.join(buildir, './env-shell.sh')
                        mergescript = os.path.join(buildir, 'hdfwriter/resources/scripts/merge.py')

                        mergeReturn = sub.call([envpath,
                                            "python", mergescript,
                                            "%s"%hdf5Files, "-o %s"%hdf5Out])
                    
                    if mergeReturn : verified = 0

                    if not dryrun:
                        dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","PERMANENT","%s","%s")\
                               on duplicate key update dataset_id="%s",queue_id="%s",name="%s",path="%s",type="PERMANENT",md5sum="%s",size="%s",transferstate="WAITING"  """% \
                                         (DDatasetId,nRecord['queue_id'],os.path.basename(hdf5Out),"file:"+os.path.dirname(hdf5Out)+"/",str(FileTools(hdf5Out, logger).md5sum()),str(os.path.getsize(hdf5Out)),\
                                          DDatasetId,nRecord['queue_id'],os.path.basename(hdf5Out),"file:"+os.path.dirname(hdf5Out)+"/",str(FileTools(hdf5Out, logger).md5sum()),str(os.path.getsize(hdf5Out))))
                        
                        
                        dbs4_.execute("""update i3filter.urlpath set transferstate="IGNORED" where dataset_id=%s and name like "%%%s%%hdf5%%" and name not like "%%Merged%%" """%(DDatasetId,RunId))

               
            if verified:
                logger.info("Succesfully Verified processing for run %s" % RunId)
            else:
                logger.error("Failed Verification for run %s, see other logs" % RunId)
            
            if not args.dryrun:
                set_post_processing_state(RunId, DDatasetId, verified, dbs4_, args.dryrun, logger)

        except Exception,err:
            logger.exception(err)
            logger.warning("skipping verification for %s, see previous error" % RunId)

if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)

    parser.add_argument("--sourcedatasetid", type=int, required = True,
                                      dest="SDATASETID", help="Dataset ID to read from, usually L2 dataset")
    
    parser.add_argument("--destinationdatasetid", type=int, required = True,
                                      dest="DDATASETID", help="Dataset ID to write to, usually L3 dataset")


    parser.add_argument("-s", "--startrun", type=int, required = True,
                                      dest="STARTRUN", help="start submission from this run")


    parser.add_argument("-e", "--endrun", type=int, default=9999999999,
                                      dest="ENDRUN", help="end submission at this run")
    
    parser.add_argument("--mergehdf5", action="store_true", default=False,
              dest="MERGEHDF5", help="merge hdf5 files, useful when files are really small")
    
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(sublogpath = 'L3Processing'), "PostProcessing_%s_%s_" % (args.SDATASETID, args.DDATASETID))
    logger = get_logger(args.loglevel,LOGFILE)

    main(args, logger)
    
