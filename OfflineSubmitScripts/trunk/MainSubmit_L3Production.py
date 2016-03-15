#!/usr/bin/env python


#############################################################################
#
#  General Description: creates entries in 'url_path', 'jobs', and 'run' tables in dbs4
#  to be used by IceProd in creating/submitting L3 jobs
#
#
# Copyright: (C) 2014 The IceCube collaboration
#
# @file    $Id$
# @version $Revision$

# @date    06/30/2014
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

    parser.add_option("--sourcedatasetid", type="int", 
                                      dest="SDATASETID_", help="Dataset ID to read from, usually L2 dataset")
    
    parser.add_option("--destinationdatasetid", type="int", 
                                      dest="DDATASETID_", help="Dataset ID to write to, usually L3 dataset")


    parser.add_option("-s", "--startrun", type="int", default=0,
                                      dest="STARTRUN_", help="start submission from this run")


    parser.add_option("-e", "--endrun", type="int", default=0,
                                      dest="ENDRUN_", help="end submission at this run")
    
    
    parser.add_option("--outdir", type="str", default="/data/ana/Muon/level3/",
                                      dest="OUTDIR_", help="main output directory")
    
    
    parser.add_option("-a", "--aggregate", type="int", default=1,
                                      dest="AGGREGATE_", help="number of subruns to aggregate to form one job, needed when processing 1 subrun is really short")


    parser.add_option("-c", "--cleandatawarehouse", action="store_true", default=False,
              dest="CLEANDW_", help="clean output files in datawarehouse as part of (re)submission process")


    parser.add_option("-r", "--dryrun", action="store_true", default=False,
              dest="DRYRUN_", help="don't set status, just print the runs to be affected")


    try:
        SubmissionLogFile = "./L3_RunSubmission_"+time.strftime("%Y_%m_%d-%H_%M_%S",time.localtime()) + ".log"
    except:
        SubmissionLogFile = "./L3_RunSubmission.log"

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

    
    params["SDatasetId"] = options.SDATASETID_
    
    params["DDatasetId"] = options.DDATASETID_

    params["START_RUN"] = options.STARTRUN_

    params["END_RUN"] = options.ENDRUN_
    
    params["OUTDIR"] = options.OUTDIR_
    
    params["AGGREGATE"] = options.AGGREGATE_

    params["CLEAN_DW"] = options.CLEANDW_

    params["DryRun"] = options.DRYRUN_

    params["SUBMITLOG"] = SUBMITLOG

    return params


def CleanRunL3(DatasetId,Run,CLEAN_DW):
    

    try:
        
        print """ select j.queue_id from i3filter.job j
                              join i3filter.run r on j.queue_id=r.queue_id
                              where r.dataset_id=%s and j.dataset_id=%s
                              and r.run_id=%s"""\
                              %(DatasetId,DatasetId,Run)
        
        tmp  = dbs4_.fetchall(""" select j.queue_id from i3filter.job j
                              join i3filter.run r on j.queue_id=r.queue_id
                              where r.dataset_id=%s and j.dataset_id=%s
                              and r.run_id=%s"""\
                              %(DatasetId,DatasetId,Run) )

      
        if not len(tmp):
            return 
        if len(tmp):
            CleanListStr = ",".join([str(t[0]) for t in tmp])

            #optional: also delete exisitng output files in data warehouse
            if CLEAN_DW:
                print """ SELECT path,name FROM i3filter.urlpath
                                                         where dataset_id=%s and queue_id in (%s) and type!="INPUT" """%(DatasetId,CleanListStr)
                # clean only output files, exclude INPUT = {PFFilt, GCD} files
                tmp1 = dbs4_.fetchall(""" SELECT path,name FROM i3filter.urlpath
                                                         where dataset_id=%s and queue_id in (%s) and type!="INPUT" """%(DatasetId,CleanListStr))
        

                if len(tmp1):
                    for t in tmp1:
                        filename = t[0][5:]+"/"+t[1]
                        if os.path.isfile(filename):
                            print "deleting ", filename
                            os.system("rm %s"%filename)
                            
            dbs4_.execute("""delete from i3filter.job where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))
            print """delete from i3filter.job where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr)
            dbs4_.execute("""delete from i3filter.urlpath where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))
            print """delete from i3filter.urlpath where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr)
            dbs4_.execute("""delete from i3filter.run where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))
            print """delete from i3filter.run where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr)
        
            #end: optional delete files in data warehouse
        

    except Exception,err:
        raise Exception("Error: %s\n"%str(err))



def GetRunStatus(GRLInfo):
    reason = GRLInfo['reason_i3'] + GRLInfo['reason_it']
    
    if GRLInfo['good_i3'] and  GRLInfo['good_it'] and GRLInfo['GCDCheck']\
       and GRLInfo['BadDOMsCheck'] and GRLInfo['FilesComplete']:
        status = "WAITING"
    
    elif 'failed' in reason:
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

## "/net/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2014/IC86_2014.dat"
#def GetExistingChkSums(ChkSumFile="/net/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2014/IC86_2014.dat"):    
#    # get dictionary of precalculated chksums for PFFilt files, caching makes submission faster
#    ExistingChkSums = {}
#    try:
#        ExistingChkSums = cPickle.load(open(ChkSumFile,"rb"))
#        return ExistingChkSums
#    except Exception,err:
#        return ExistingChkSums
#        #raise Exception("Error: %s\n"%str(err))


#def SubmitRun(g,status,DatasetId,QueueId,ExistingChkSums,SUBMITLOG):
def SubmitRunL3(DDatasetId,SDatasetId,Run,QId,OUTDIR,AGGREGATE,SUBMITLOG):

        
        runInfo = dbs4_.fetchall("""select r.date,r.sub_run,u.* from i3filter.job j
                                    join i3filter.run r on r.queue_id=j.queue_id
                                    join i3filter.urlpath u on u.queue_id=j.queue_id
                                    where j.dataset_id=%s and r.dataset_id=%s and u.dataset_id=%s
                                    and (u.type="PERMANENT" or name like "%%GCD%%" ) and r.run_id=%s and j.status !="BadRun"
                                    order by r.sub_run
                                    """%(SDatasetId,SDatasetId,SDatasetId,str(Run)),UseDict=True)

            
        if not len(runInfo):
            print "no input L2 files for this run %s, something must be wrong"%str(Run)
            exit(1)
            
        date_ = runInfo[0]['date']
        date_ = str(date_.year)+ "/"+str(date_.month).zfill(2)+str(date_.day).zfill(2)
        OutDir = os.path.join(OUTDIR,date_,"Run00"+str(Run))

        if not os.path.exists(OutDir):
            os.makedirs(OutDir)
        
        
        firstSubRun = runInfo[0]['sub_run']
        lastSubRun = runInfo[-1]['sub_run']
        
        if AGGREGATE <=0:
            print "aggregate variable must be integer >=1"
            exit(1)
        elif AGGREGATE > 1:
            groups_ = range(firstSubRun,lastSubRun,AGGREGATE)
        else:
            groups_ = range(firstSubRun,lastSubRun+1)
            
        
        GCDEntry = [g for g in runInfo if g['sub_run']==1 and "GCD" in g['name']][0]
        GCDFile = os.path.join(GCDEntry['path'][5:],GCDEntry['name'])
        lnCmd = "ln -sf %s %s"%(GCDFile,os.path.join(OutDir,os.path.basename(GCDFile)))
        print lnCmd
        os.system(lnCmd)
        print groups_ 

        for g in range(len(groups_)-1):
            QId+=1
            print range(groups_[g],groups_[g+1])
            print """insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"WAITING")"""%(DDatasetId,QId)
            dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"WAITING")"""%(DDatasetId,QId))
         
            print """insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
               (DDatasetId,QId,GCDEntry['name'],GCDEntry['path'],GCDEntry['md5sum'],GCDEntry['size'])
            dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
               (DDatasetId,QId,GCDEntry['name'],GCDEntry['path'],GCDEntry['md5sum'],GCDEntry['size']))
        
         
            p = [r for r in runInfo if r['sub_run'] in range(groups_[g],groups_[g+1])and str(r['sub_run']).zfill(8)+"_" not in r['name'] and r['type']=="PERMANENT"]
            
            for q in p:
                #print q
                
                print """insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DDatasetId,QId,q['name'],q['path'],q['md5sum'],q['size'])
                
                dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DDatasetId,QId,q['name'],q['path'],q['md5sum'],q['size']))
        
            
            print """insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(Run,DDatasetId,QId,p[0]['sub_run'],q['date'])
            dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(Run,DDatasetId,QId,p[0]['sub_run'],q['date']))
             
            print "=========\n"
        
        QId+=1
        print range(groups_[g+1],lastSubRun+1)
        
        print """insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"WAITING")"""%(DDatasetId,QId)
        dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"WAITING")"""%(DDatasetId,QId))
         
        print """insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
               (DDatasetId,QId,GCDEntry['name'],GCDEntry['path'],GCDEntry['md5sum'],GCDEntry['size'])
        dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
           (DDatasetId,QId,GCDEntry['name'],GCDEntry['path'],GCDEntry['md5sum'],GCDEntry['size']))
        
        
        p = [r for r in runInfo if r['sub_run'] in range(groups_[g+1],lastSubRun+1) and str(r['sub_run']).zfill(8)+"_" not in r['name'] and r['type']=="PERMANENT"]
        for q in p:
            
            print """insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DDatasetId,QId,q['name'],q['path'],q['md5sum'],q['size'])
                
            dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DDatasetId,QId,q['name'],q['path'],q['md5sum'],q['size']))
            #print q
            #print str(q['sub_run']).zfill(8)
            
        print """insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(Run,DDatasetId,QId,p[0]['sub_run'],q['date'])
        dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(Run,DDatasetId,QId,p[0]['sub_run'],q['date']))
             
        
        #p = [r for r in runInfo if runInfo['sub_run'] in range(groups_[g],groups_[g+1]) ]
        
        #for r in runInfo:
        #    QId+=1
        #    print SDatasetId,Run,r['sub_run'],r['name'],r['path'],r['type'],r['md5sum'],r['size']
        #    
        #    print """insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"IDLE")"""%(DDatasetId,QId)
        #    #dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"IDLE")"""%(DDatasetId,QId))
        #
        #    #dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
        #    #                     (DatasetId,QueueId,os.path.basename(GCDFileName),"file:"+os.path.dirname(GCDFileName)+"/",GCDFileChkSum,str(os.path.getsize(GCDFileName))))
        #
            
        #OutputDir = "/net/user/i3filter/L3_Processing/TestOutput"
    #    SUBMITLOG.write("""Submitting Run = %s \n"""%(Run))
    #    InFiles = []
    #
    #    sDay = g['tStart']      # run start date
    #    sY = sDay.year
    #    sM = str(sDay.month).zfill(2)
    #    sD = str(sDay.day).zfill(2)
    #    
    #    R = RunTools(g['run_id'])
    #    InFiles = R.GetRunFiles(g['tStart'],'P')        
    #    #InFiles = GetRunFiles(g['run_id'],sDay,"P")
    #
    #    MainOutputDir = OutputDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/"%(sY,sM,sD)
    #    if not os.path.exists(MainOutputDir):
    #        os.mkdir(MainOutputDir)
    #    
    #    
    #    OutputDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/Run00%s_%s"%(sY,sM,sD,g['run_id'],g['production_version'])
    #    if not os.path.exists(OutputDir):
    #        os.mkdir(OutputDir)
    #    
    #    
    #    GCDFileName = []
    #    GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/VerifiedGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
    #    
    #    if not len(GCDFileName): GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/AllGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
    #    
    #    if len(GCDFileName):
    #        GCDFileName = GCDFileName[0]
    #        #print GCDFileName
    #        #GCDFileChkSum = str(md5sum(GCDFileName))
    #        GCDFileChkSum = str(FileTools(GCDFileName).md5sum())
    #        
    #        lnGCDFile = os.path.join(OutputDir,os.path.basename(GCDFileName))
    #        lnCmd = "ln -sf %s %s"%(GCDFileName,lnGCDFile)
    #        #print lnCmd
    #        os.system(lnCmd)
    #    else:
    #        GCDFileName = ""
    #
    #    
    #    if not len(InFiles):
    #        SUBMITLOG.write("No PFFilt will be submitted for run %s\n"%g['run_id'])
    #        print "No PFFilt Files will be submitted for run %s"%g['run_id']
    #    
    #        QueueId+=1
    #        dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
    #        dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,-1,str(sDay.date())))
    #    
    #    
    #    else:
    #        SUBMITLOG.write("Attempting to submit %s PFFilt Files for run %s\n"%(str(len(InFiles)),g['run_id']))
    #        print "Attempting to submit %s PFFilt Files for run %s"%(str(len(InFiles)),g['run_id'])
    #    
    #        CountSubRun = 0
    #        for InFile in InFiles:
    #            QueueId+=1
    #    
    #            dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
    #    
    #            if os.path.isfile(GCDFileName):
    #                dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
    #                             (DatasetId,QueueId,os.path.basename(GCDFileName),"file:"+os.path.dirname(GCDFileName)+"/",GCDFileChkSum,str(os.path.getsize(GCDFileName))))
    #    
    #    
    #            if InFile in ExistingChkSums:
    #                InFileChkSum = str(ExistingChkSums[InFile])
    #            else:
    #                #InFileChkSum = str(md5sum(InFile))
    #                InFileChkSum = str(FileTools(InFile).md5sum())
    #    
    #            dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
    #                         (DatasetId,QueueId,os.path.basename(InFile),"file:"+os.path.dirname(InFile)+"/",InFileChkSum,str(os.path.getsize(InFile))))
    #    
    #    
    #            dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,CountSubRun,str(sDay.date())))
    #            CountSubRun+=1
        
        



def main(params):
    
    SUBMITLOG = params["SUBMITLOG"]
    SDatasetId = params["SDatasetId"]
    DDatasetId = params["DDatasetId"]
    START_RUN = params["START_RUN"]
    END_RUN = params["END_RUN"]
    OUTDIR = params["OUTDIR"]
    AGGREGATE = params["AGGREGATE"]
    CLEAN_DW = params["CLEAN_DW"]
    DryRun = params["DryRun"]
    
    AllRuns = []
    
    MARKED_RUNS = []
    if (END_RUN) and  (START_RUN) and END_RUN >= START_RUN:
        MARKED_RUNS = range(START_RUN,END_RUN+1)
    
    AllRuns.extend(MARKED_RUNS)


    sourceInfo = dbs4_.fetchall("""select r.run_id from i3filter.job j
                                        join i3filter.run r on j.queue_id=r.queue_id
                                        where j.dataset_id=%s and r.dataset_id=%s
                                        and r.run_id between %s and %s
                                        and j.status="OK"
                                        group by r.run_id
                                        order by r.run_id,j.queue_id
                                        """%(SDatasetId,SDatasetId,START_RUN,END_RUN),UseDict=True)

    print len(sourceInfo)

    #if not len(AllRuns):
    #    SUBMITLOG.write("No new runs to submit or old to update, check start:%s and end:%s run arguments"%(START_RUN,END_RUN))
    #    exit(0)
    
    for s in sourceInfo:
        if DryRun:
            print s
            
        else:
            SUBMITLOG.write("\n**************\nAttempting to (Re)submit %s \n"%(s))
            #print Run
    
            CleanRunL3(DDatasetId,s['run_id'],CLEAN_DW)
            
            QId = MaxQId(DDatasetId)
            
            SubmitRunL3(DDatasetId,SDatasetId,s['run_id'],QId,OUTDIR,AGGREGATE,SUBMITLOG)
            

            #runInfo = dbs4_.fetchall("""select j.queue_id from i3filter.job j
            #                        join i3filter.run r on r.queue_id=j.queue_id
            #                        where j.dataset_id=%s and r.dataset_id=%s
            #                        and r.run_id=%s
            #                        order by j.queue_id
            #                        """%(SDatasetId,SDatasetId,s['run_id']),UseDict=True)
            
            #print runInfo
    
    #ExistingChkSums = GetExistingChkSums()
    #
    #for Run in AllRuns:
    #
    #    if DryRun:
    #        print Run
    #
    #    else:
    #        SUBMITLOG.write("\n**************\nAttempting to (Re)submit %s \n"%(Run))
    #        #print Run
    #
    #        GRLInfo = dbs4_.fetchall("""select g.*,r.tStart,r.FilesComplete from i3filter.grl_snapshot_info g
    #                                join i3filter.run_info_summary r on r.run_id=g.run_id
    #                                where g.run_id=%s and not submitted"""%(Run),UseDict=True)
    #    
    #        if not len(GRLInfo):
    #            print "run %s already submitted or no information for new submission "%Run
    #            continue
    #        
    #        for g in GRLInfo:
    #            status = GetRunStatus(g)
    #           
    #            CleanRun(DatasetId,Run,CLEAN_DW,g)
    #        
    #            QId = MaxQId(DatasetId)
    #            
    #            SubmitRun(g,status,DatasetId,QId,ExistingChkSums,SUBMITLOG)
    #            
    ##            dbs4_.execute("""update i3filter.grl_snapshot_info\
    ##                             set submitted=1 \
    ##                             where run_id=%s and production_version=%s"""%\
    ##                             (g['run_id'],g['production_version']))
    #
    #            SUBMITLOG.write("**************\n")
    #
    #SUBMITLOG.close()

if __name__ == '__main__':

    params = GetParams()
    if not params["SDatasetId"] or not params["DDatasetId"]:
        print "you must enter source and destination dataset_ids for submission"
        exit(1)


    main(params)
