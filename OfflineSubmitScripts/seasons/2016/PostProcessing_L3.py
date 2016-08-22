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

from libs.files import get_logdir, get_tmpdir, write_meta_xml_post_processing
import libs.config
sys.path.append(libs.config.get_config().get('DEFAULT', 'SQLClientPath'))
sys.path.append(libs.config.get_config().get('DEFAULT', 'ProductionToolsPath'))
from RunTools import *
from FileTools import *

from libs.logger import get_logger, delete_log_file
from libs.argparser import get_defaultparser
from libs.runs import set_post_processing_state, get_validated_runs
import libs.process

##-----------------------------------------------------------------
## setup DB
##-----------------------------------------------------------------

import SQLClient_i3live as live
m_live = live.MySQL()

import SQLClient_dbs4 as dbs4
dbs4_ = dbs4.MySQL()

import SQLClient_dbs2 as dbs2
dbs2_ = dbs2.MySQL()

def main(SDatasetId, DDatasetId, START_RUN, END_RUN, MERGEHDF5, NOMETADATA, dryrun, logger):
    SEASON = libs.config.get_season_by_run(START_RUN)

    season_of_end_run = libs.config.get_season_by_run(END_RUN)

    if SEASON != season_of_end_run:
        logger.warning("The first run (%s) is of season %s, the last run (%s) is of season %s. Only runs of season %s will be post processed." % (
            START_RUN, SEASON, END_RUN, season_of_end_run, SEASON
        ))

    sourceRunInfo = dbs4_.fetchall("""SELECT r.run_id FROM i3filter.job j
                                        JOIN i3filter.run r ON j.queue_id=r.queue_id
                                        WHERE j.dataset_id=%s AND r.dataset_id=%s
                                        AND r.run_id BETWEEN %s AND %s
                                        AND j.status="OK"
                                        GROUP BY r.run_id
                                        ORDER BY r.run_id
                                        """%(SDatasetId,SDatasetId,START_RUN,END_RUN),UseDict=True)
   
    counter = {'all': 0, 'validated': 0, 'errors': 0, 'skipped': 0}
 
    GRL = "/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo.txt"%(SEASON,SEASON)
    if not os.path.isfile(GRL):
        logger.critical("Can't access GRL file %s for run validation, check path, exiting ...... " % GRL)
        return counter
        
    with open(GRL,"r") as G:
        L = G.readlines()
        GoodRuns = [int(l.split()[0]) for l in L if l.split()[0].isdigit()]

    # Get validated runs in order to avoid validating them another time
    validated_runs = {}
    for run in get_validated_runs(DDatasetId, dbs4_, True, logger):
        validated_runs[int(run['run_id'])] = run

    for s in sourceRunInfo:
        counter['all'] = counter['all'] + 1

        try:    
            verified = 1
            
            RunId = s['run_id']
            
            if not RunId in GoodRuns:
                logger.info("Skip run %s since it is not in the good run list" % RunId)
                counter['skipped'] = counter['skipped'] + 1
                continue

            if int(RunId) in validated_runs:
                logger.debug("Run %s was already validated on %s" % (RunId, validated_runs[int(RunId)]['date_of_validation']))
                counter['skipped'] = counter['skipped'] + 1
                continue
            
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

            if not len(dRunInfo):
                logger.info("No processing information found in DB. Run %s may hav not been submitted yet. Skip this run." % RunId)
                counter['skipped'] = counter['skipped'] + 1
                continue

            logger.info("Verifying processing for run %s..." % RunId)
        
            # Output directory
            # Look for PERMANENT entries in urlpath
            outdir = None
            for entry in dRunInfo:
                if entry['type'] == 'PERMANENT':
                    outdir = entry['path']
                    break

            if outdir is None:
                logger.critical("Could not determine output directory")
                counter['errors'] = counter['errors'] + 1
                return counter

            # Since there is a 'file:' at the beginning of the path...
            outdir = outdir[5:]

            logger.debug("outdir: %s" % outdir)

            # Check GCD file in L3 out dir.
            gInfo = [s for s in sRunInfo if "GCD" in s['name']] # GCD file from L2 source
            gInfo = gInfo[0]
            
            # get GCD file linked from L3 dir.
            linkedGCD = glob.glob(os.path.join(outdir, '*GCD*'))
            if not len(linkedGCD):
                verified = 0
                logger.error("no GCD file linked from out dir. for run %s" % RunId)
            elif not os.path.isfile(linkedGCD[0]):
                verified = 0
                logger.error("Listed GCD file in DB not in output dir. for run %s" % RunId)
            elif gInfo['md5sum'] != FileTools(linkedGCD[0], logger).md5sum():
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
                    badRunL3 = [d for d in dRunInfo if d['name']==sr['name']]   
                    if len(badRunL3) and badRunL3[0]['status'] == "BadRun":
                        logger.info("Skipped sub run %s since it is declared as bad in L3" % sr['sub_run'])
                        continue

                    # Sometimes a sub run has been discarded in L2 post processing
                    logger.debug("sr = %s" % sr)
                    if sr['status'] == "BadRun":
                        logger.info("Skipped sub run %s since it is declared as bad in L2" % sr['sub_run'])
                        continue

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

                    if not dryrun:
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
                if not NOMETADATA:
                    dest_folder = ''
                    if dryrun:
                        dest_folder = get_tmpdir()
                    else:
                        dest_folder = outdir

                    write_meta_xml_post_processing(dest_folder = dest_folder,
                                                   level = 'L3',
                                                   script_file = __file__,
                                                   logger = logger)
                    
                else:
                    logger.info("No meta data files will be written")

                logger.info("Succesfully Verified processing for run %s" % RunId)
                counter['validated'] = counter['validated'] + 1
            else:
                logger.error("Failed Verification for run %s, see other logs" % RunId)
                counter['errors'] = counter['errors'] + 1
            
            if not dryrun:
                set_post_processing_state(RunId, DDatasetId, verified, dbs4_, dryrun, logger)

        except Exception,err:
            logger.exception(err)
            counter['errors'] = counter['errors'] + 1
            logger.warning("skipping verification for %s, see previous error" % RunId)

    logger.info("%s runs were handled | validated %s runs | errors: %s | skipped %s runs" % (counter['all'], counter['validated'], counter['errors'], counter['skipped']))
    return counter

if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)

    parser.add_argument("--sourcedatasetid", type=int, default = None,
                                      dest="SDATASETID", help="Dataset ID to read from, usually L2 dataset. Required if not executed with --cron. If executed with --cron, this option is ignored")
    
    parser.add_argument("--destinationdatasetid", type=int, default = None,
                                      dest="DDATASETID", help="Dataset ID to write to, usually L3 dataset. Required if not executed with --cron. If executed with --cron, this option is ignored")


    parser.add_argument("-s", "--startrun", type=int, default = None,
                                      dest="STARTRUN", help="start submission from this run. Required if not executed with --cron. If executed with --cron, this option is ignored")


    parser.add_argument("-e", "--endrun", type=int, default=9999999999,
                                      dest="ENDRUN", help="end submission at this run. If executed with --cron, this option is ignored")
    
    parser.add_argument("--mergehdf5", action="store_true", default=False,
              dest="MERGEHDF5", help="merge hdf5 files, useful when files are really small")
    
    parser.add_argument("--nometadata", action="store_true", default=False,
              dest="NOMETADATA", help="Don't write meta data files")

    parser.add_argument("--cron", action="store_true", default=False, dest="CRON", help="Execute as cron. No other options required")

    args = parser.parse_args()

    LOGFILE = None

    if args.CRON:
        LOGFILE=os.path.join(get_logdir(sublogpath = 'L3Processing'), "PostProcessing_CRON_")
    else:
        LOGFILE = os.path.join(get_logdir(sublogpath = 'L3Processing'), "PostProcessing_%s_%s_" % (args.SDATASETID, args.DDATASETID))

    logger = get_logger(args.loglevel,LOGFILE)

    if not args.CRON and (args.SDATASETID is None or args.DDATASETID is None or args.STARTRUN is None):
        logger.critical("--sourcedatasetid and --destinationdatasetid and -s are required if not executed with --cron")
        exit(1)

    lock = None
    if args.CRON:
        if not libs.config.get_config().getboolean('L3', 'CronPostProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = libs.process.Lock(os.path.basename(__file__), logger)
        lock.lock()

    if not args.CRON:
        main(SDatasetId = args.SDATASETID, 
            DDatasetId = args.DDATASETID,
            START_RUN = args.STARTRUN, 
            END_RUN = args.ENDRUN, 
            MERGEHDF5 = args.MERGEHDF5, 
            NOMETADATA = args.NOMETADATA, 
            dryrun = args.dryrun, 
            logger = logger)
    else:
        # Find installed crons
        crons = libs.config.get_var_dict('L3', 'CronJobPostProcessing', keytype = int, valtype = int)

        firstrun = libs.config.get_config().get('L3', 'CronRunStart')
        lastrun = libs.config.get_config().get('L3', 'CronRunEnd')

        logger.debug("crons: %s" % crons)

        for dest, source in crons.iteritems():
            logger.info('====================================================')
            logger.info("Executing Cron Job for dataset %s with source %s" % (dest, source))

            counter = main(SDatasetId = source, 
                DDatasetId = dest,
                START_RUN = firstrun, 
                END_RUN = lastrun, 
                MERGEHDF5 = args.MERGEHDF5,
                NOMETADATA = args.NOMETADATA, 
                dryrun = args.dryrun, 
                logger = logger)

            # counter contains how many runs have been submitted. Since
            # the cron is executed very often and will probably do nothing
            # we won't keep those log files since they are useless.
            # Therefore, we will delete the log file if no run has been submitted
            if not (counter['validated'] + counter['errors']):
                delete_log_file(logger)
    
    if args.CRON:
        lock.unlock()

