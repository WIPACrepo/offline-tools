#!/usr/bin/env python


#############################################################################
#
#  General Description: creates PFFilt entries in 'url_path', 'jobs', and 'run' tables in dbs4
# to be used by IceProd in creating/submitting L2 jobs
#
#
# Copyright: (C) 2015 The IceCube collaboration
#
# @file    $Id$
# @version $Revision$

# @date    05/04/2015
# @author  Oladipo Fadiran <ofadiran@icecube.wisc.edu>
#
#############################################################################

import sys, os
from os.path import expandvars, join, exists
import glob
from optparse import OptionParser
import time
import datetime
#import MySQLdb
import pymysql as MySQLdb
import cPickle
import datetime
from dateutil.relativedelta import *

from RunTools import *
from FileTools import *


##-----------------------------------------------------------------
## setup DB
##-----------------------------------------------------------------

try:
    import SQLClient_i3live as live
    m_live = live.MySQL()

    import SQLClient_dbs4 as dbs4
    dbs4_ = dbs4.MySQL()
    
    import SQLClient_dbs2 as dbs2
    dbs2_ = dbs2.MySQL()

except Exception, err:
    raise Exception("Error: %s "%str(err))


##################################################################
# Retrieve arguments
##################################################################
def GetParams():
    params = {}
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)

    parser.add_option("-d", "--datasetid", type="int", default=1883,
                                      dest="DATASETID_", help="Dataset ID")


    parser.add_option("-s", "--startrun", type="int", default=0,
                                      dest="STARTRUN_", help="start submission from this run")


    parser.add_option("-e", "--endrun", type="int", default=0,
                                      dest="ENDRUN_", help="end submission at this run")


    parser.add_option("-c", "--cleandatawarehouse", action="store_true", default=False,
              dest="CLEANDW_", help="clean output files in datawarehouse as part of (re)submission process")


    parser.add_option("-r", "--dryrun", action="store_true", default=False,
              dest="DRYRUN_", help="don't set status, just print the runs to be affected")


    try:
        SubmissionLogFile = "./RunSubmission_"+time.strftime("%Y_%m_%d-%H_%M_%S",time.localtime()) + ".log"
    except:
        SubmissionLogFile = "./RunSubmission.log"

    parser.add_option("-o", "--outputlog", default=SubmissionLogFile,
                                      dest="OUTPUTLOG_", help="submission log file, default is time-stamped file name with submission location as default")


    #-----------------------------------------------------------------
    # Parse cmd line args, exit if anything is not understood
    #-----------------------------------------------------------------

    (options,args) = parser.parse_args()
    if len(args) != 0:
        message = "Got undefined options:"
        for a in args:
            message += a
            message += " "
        parser.error(message)


    ##-----------------------------------------------------------------
    ## Check and store arguments
    ##-----------------------------------------------------------------
    #

    OUTPUTLOG = options.OUTPUTLOG_

    SUBMITLOG = open(OUTPUTLOG,'w')
    if os.access(OUTPUTLOG,os.R_OK) == False:
        raise "cannot access submission log %s for writing! Please check permissions" % OUTPUTLOG

    params["DatasetId"] = options.DATASETID_

    params["START_RUN"] = options.STARTRUN_

    params["END_RUN"] = options.ENDRUN_

    params["CLEAN_DW"] = options.CLEANDW_

    params["DryRun"] = options.DRYRUN_

    params["SUBMITLOG"] = SUBMITLOG

    return params


def CleanRun(DatasetId,Run,CLEAN_DW,g):

    try:
        tmp  = dbs4_.fetchall(""" select j.queue_id from i3filter.job j
                              join i3filter.run r on j.queue_id=r.queue_id
                              join i3filter.grl_snapshot_info g on r.run_id=g.run_id
                              where r.dataset_id=%s and j.dataset_id=%s
                              and r.run_id=%s and g.production_version=%s"""\
                              %(DatasetId,DatasetId,Run,g['production_version']) )

        if len(tmp):
            CleanListStr=""
            for t in tmp:
                CleanListStr+=(str(t[0])+",")
            CleanListStr = CleanListStr[:-1]


            #optional: also delete exisitng output files in data warehouse
            if CLEAN_DW:
                # clean only output files, exclude INPUT = {PFFilt, GCD} files
                tmp = dbs4_.fetchall(""" SELECT path,name FROM i3filter.urlpath
                                         where dataset_id=%s and queue_id in (%s) and type!="INPUT"
                                                         """%(DatasetId,CleanListStr))

                
                if len(tmp):
                    for t in tmp:
                        filename = t[0][5:]+"/"+t[1]
                        if os.path.isfile(filename):
                            print "deleting ", filename
                            os.system("rm -f %s"%filename)


                         #end: optional delete files in data warehouse


            dbs4_.execute("""delete from i3filter.job where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))

            dbs4_.execute("""delete from i3filter.urlpath where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))

            dbs4_.execute("""delete from i3filter.run where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))

    except Exception,err:
        raise Exception("Error: %s\n"%str(err))



def GetRunStatus(GRLInfo):
    reason = GRLInfo['reason_i3'] + GRLInfo['reason_it']
    
    if (GRLInfo['good_i3'] or  GRLInfo['good_it']) and GRLInfo['GCDCheck']\
       and GRLInfo['BadDOMsCheck'] and GRLInfo['FilesComplete']:
        status = "WAITING"
    
    elif 'failed' in reason or 'spoiled' in reason:
        status="FailedRun"
    elif 'short' in reason:
        status='IDLEShortRun'
    elif 'test' in reason:
        status='IDLETestRun'
    elif 'lid' in reason:
        status = 'IDLELid'
            
    elif not GRLInfo['GCDCheck']:
        status = 'IDLENoGCD'

    elif not GRLInfo['BadDOMsCheck']:
        status = 'IDLEBDList'
        
    elif not GRLInfo['FilesComplete']:
        status = 'IDLEIncompleteFiles'
        
    else:
        status = "IDLE"

    return status

def MaxQId(DatasetId):
    # Get current maximum queue_id for dataset_id, any subsquent submissions starts from queue_id+1
    try:
        tmp = dbs4_.fetchall("""select max(queue_id) from i3filter.job where dataset_id=%s """%(DatasetId))
        #QueueId = int(tmp[0])
        return tmp[0][0]
    except Exception,err:
        raise Exception("Error: %s\n"%str(err))

# end: Get current maximum queue_id
#
#

# change the path once you start generating cache for IC86_2015 files
def GetExistingChkSums(ChkSumFile="/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/IC86_2015.dat"):    
    # get dictionary of precalculated chksums for PFFilt files, caching makes submission faster
    ExistingChkSums = {}
    try:
        ExistingChkSums = cPickle.load(open(ChkSumFile,"rb"))
        return ExistingChkSums
    except Exception,err:
        return ExistingChkSums
        #raise Exception("Error: %s\n"%str(err))


def SubmitRun(g,status,DatasetId,QueueId,ExistingChkSums,SUBMITLOG):

    try:
        SUBMITLOG.write("""Submitting Run = %s , Current Status = %s\n"""%(g['run_id'],status))
        InFiles = []

        sDay = g['tStart']      # run start date
        sY = sDay.year
        sM = str(sDay.month).zfill(2)
        sD = str(sDay.day).zfill(2)
        
        R = RunTools(g['run_id'])
        InFiles = R.GetRunFiles(g['tStart'],'P')        
        #InFiles = GetRunFiles(g['run_id'],sDay,"P")
 
        MainOutputDir = OutputDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/"%(sY,sM,sD)
        if not os.path.exists(MainOutputDir):
            os.mkdir(MainOutputDir)
        
        
        OutputDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/Run00%s_%s"%(sY,sM,sD,g['run_id'],g['production_version'])
        if not os.path.exists(OutputDir):
            os.mkdir(OutputDir)
        
        
        GCDFileName = []
        GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/VerifiedGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
        
        if not len(GCDFileName): GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/AllGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
        
        if len(GCDFileName):
            GCDFileName = GCDFileName[0]
            #print GCDFileName
            #GCDFileChkSum = str(md5sum(GCDFileName))
            GCDFileChkSum = str(FileTools(GCDFileName).md5sum())
            
            lnGCDFile = os.path.join(OutputDir,os.path.basename(GCDFileName))
            lnCmd = "ln -sf %s %s"%(GCDFileName,lnGCDFile)
            #print lnCmd
            os.system(lnCmd)
        else:
            GCDFileName = ""
    
        
        if not len(InFiles):
            SUBMITLOG.write("No PFFilt will be submitted for run %s\n"%g['run_id'])
            print "No PFFilt Files will be submitted for run %s"%g['run_id']
        
            QueueId+=1
            dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
            dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,-1,str(sDay.date())))
        
        
        else:
            SUBMITLOG.write("Attempting to submit %s PFFilt Files for run %s\n"%(str(len(InFiles)),g['run_id']))
            print "Attempting to submit %s PFFilt Files for run %s"%(str(len(InFiles)),g['run_id'])
        
            #CountSubRun = 0
            for InFile in InFiles:
                
                CountSubRun = int(InFile[len(InFile)-16:-8])
                
                QueueId+=1
        
                dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
        
                if os.path.isfile(GCDFileName):
                    dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                                 (DatasetId,QueueId,os.path.basename(GCDFileName),"file:"+os.path.dirname(GCDFileName)+"/",GCDFileChkSum,str(os.path.getsize(GCDFileName))))
        
        
                if InFile in ExistingChkSums:
                    InFileChkSum = str(ExistingChkSums[InFile])
                else:
                    #InFileChkSum = str(md5sum(InFile))
                    InFileChkSum = str(FileTools(InFile).md5sum())
        
                dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DatasetId,QueueId,os.path.basename(InFile),"file:"+os.path.dirname(InFile)+"/",InFileChkSum,str(os.path.getsize(InFile))))
        
        
                dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,CountSubRun,str(sDay.date())))
                #CountSubRun+=1
        
        
    except Exception,err:
        raise Exception("Error: %s\n"%str(err))



def main(params):

    SUBMITLOG = params["SUBMITLOG"]
    DatasetId = params["DatasetId"]
    START_RUN = params["START_RUN"]
    END_RUN = params["END_RUN"]
    CLEAN_DW = params["CLEAN_DW"]
    DryRun = params["DryRun"]

    AllRuns = []

    MARKED_RUNS = []
    if (END_RUN) and  (START_RUN) and END_RUN >= START_RUN:
        MARKED_RUNS = range(START_RUN,END_RUN+1)

    AllRuns.extend(MARKED_RUNS)

    if not len(AllRuns):
        SUBMITLOG.write("No new runs to submit or old to update, check start:%s and end:%s run arguments"%(START_RUN,END_RUN))
        exit(0)

    ExistingChkSums = GetExistingChkSums()

    for Run in AllRuns:

        if DryRun:
            print Run

        else:
            SUBMITLOG.write("\n**************\nAttempting to (Re)submit %s \n"%(Run))
            #print Run

            GRLInfo = dbs4_.fetchall("""select g.*,r.tStart,r.FilesComplete from i3filter.grl_snapshot_info g
                                    join i3filter.run_info_summary r on r.run_id=g.run_id
                                    where g.run_id=%s and not submitted"""%(Run),UseDict=True)
        
            if not len(GRLInfo):
                print "run %s already submitted or no information for new submission "%Run
                continue
            
            for g in GRLInfo:
                status = GetRunStatus(g)
               
                CleanRun(DatasetId,Run,CLEAN_DW,g)
            
                QId = MaxQId(DatasetId)
                
                SubmitRun(g,status,DatasetId,QId,ExistingChkSums,SUBMITLOG)
                
                dbs4_.execute("""update i3filter.grl_snapshot_info\
                                 set submitted=1 \
                                 where run_id=%s and production_version=%s"""%\
                                 (g['run_id'],g['production_version']))

                SUBMITLOG.write("**************\n")

    SUBMITLOG.close()

if __name__ == '__main__':

    params = GetParams()

    main(params)
