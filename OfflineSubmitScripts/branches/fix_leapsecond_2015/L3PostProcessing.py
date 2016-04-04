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


    parser.add_option("-e", "--endrun", type="int", default=9999999999,
                                      dest="ENDRUN_", help="end submission at this run")
    
    
    parser.add_option("--outdir", type="str", default="",
                                      dest="OUTDIR_", help="main output directory")
    
    parser.add_option("--season", type="str", default="",
                                    dest="SEASON_", help="start year of data taking e.g. 2012 for the IC86_2012 season")
    
    parser.add_option("--mergehdf5", action="store_true", default=False,
              dest="MERGEHDF5_", help="merge hdf5 files, useful when files are really small")
    
    
    #parser.add_option("-a", "--aggregate", type="int", default=1,
    #                                  dest="AGGREGATE_", help="number of subruns to aggregate to form one job, needed when processing 1 subrun is really short")
    #
    #
    #parser.add_option("-c", "--cleandatawarehouse", action="store_true", default=False,
    #          dest="CLEANDW_", help="clean output files in datawarehouse as part of (re)submission process")
    #
    #
    #parser.add_option("-r", "--dryrun", action="store_true", default=False,
    #          dest="DRYRUN_", help="don't set status, just print the runs to be affected")


   
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

    #OUTPUTLOG = options.OUTPUTLOG_

    #SUBMITLOG = open(OUTPUTLOG,'w')
    #if os.access(OUTPUTLOG,os.R_OK) == False:
    #    raise "cannot access submission log %s for writing! Please check permissions" % OUTPUTLOG

    
    params["SDatasetId"] = options.SDATASETID_
    
    params["DDatasetId"] = options.DDATASETID_

    params["START_RUN"] = options.STARTRUN_

    params["END_RUN"] = options.ENDRUN_

    params["SEASON"] = options.SEASON_
    
    params["OUTDIR"] = options.OUTDIR_
    
    params["MERGEHDF5"] = options.MERGEHDF5_
    
    #params["AGGREGATE"] = options.AGGREGATE_

    #params["CLEAN_DW"] = options.CLEANDW_

    #params["DryRun"] = options.DRYRUN_

    #params["SUBMITLOG"] = SUBMITLOG

    return params


def notGood(msg,RunId):
    print msg
    SUBMITLOG.write("\n"+msg)
    return 0



def main(params,SUBMITLOG):
    
    #SUBMITLOG = params["SUBMITLOG"]
    SDatasetId = params["SDatasetId"]
    DDatasetId = params["DDatasetId"]
    START_RUN = params["START_RUN"]
    END_RUN = params["END_RUN"]
    SEASON = params["SEASON"]
    OUTDIR = params["OUTDIR"]
    MERGEHDF5 = params["MERGEHDF5"]
    
    #AGGREGATE = params["AGGREGATE"]
    #CLEAN_DW = params["CLEAN_DW"]
    #DryRun = params["DryRun"]
    


    sourceRunInfo = dbs4_.fetchall("""select r.run_id from i3filter.job j
                                        join i3filter.run r on j.queue_id=r.queue_id
                                        where j.dataset_id=%s and r.dataset_id=%s
                                        and r.run_id between %s and %s
                                        and j.status="OK"
                                        group by r.run_id
                                        order by r.run_id
                                        """%(SDatasetId,SDatasetId,START_RUN,END_RUN),UseDict=True)
    
    
    #sourceRunInfo = sourceRunInfo[0:4]
    #RunId = sourceRunInfo[0]['run_id']
    
    GRL = "/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo.txt"%(SEASON,SEASON)
    if not os.path.isfile(GRL):
        print "can't access GRL file %s for run validation, check path, exiting ...... "%GRL
        exit(1)
        
    with open(GRL,"r") as G:
        L = G.readlines()
        GoodRuns = [int(l.split()[0]) for l in L if l.split()[0].isdigit()]
    

    
    for s in sourceRunInfo:
    
        try:    
            verified = 1
            
            RunId = s['run_id']
            
            
            if not RunId in GoodRuns: continue
            
            #print "\n==================="
            #print "\n===================\nVerifying processing for run %s"%RunId
            SUBMITLOG.write("\n===================\nVerifying processing for run %s"%RunId)
        
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
            #
            gInfo = [s for s in sRunInfo if "GCD" in s['name']] # GCD file from L2 source
            gInfo = gInfo[0]
            
            
            # get GCD file linked from L3 dir.
            linkedGCD = []
            linkedGCD = [s for s in dRunInfo if "Level3" in s['name']]
            linkedGCD = glob.glob(linkedGCD[0]['path'].split("file:")[1]+"/*GCD*")
            if not len(linkedGCD):
                verified = notGood("no GCD file linked from out dir. for run %s"%RunId,RunId)
            if not os.path.isfile(linkedGCD[0]):
                verified = notGood("listed GCD file in DB not in output dir. for run %s"%RunId,RunId)
            if gInfo['md5sum']!=FileTools(linkedGCD[0]).md5sum():
                verified = notGood("GCD file linked from L3 dir. has different md5sum from source L2 dir. for run %s"%RunId,RunId)
                
            # End: GCD check
            
            
            
            
            sRunInfo = [s for s in sRunInfo if "EHE" not in s['name'] and "_IT" not in s['name'] and "SLOP" not in s['name'] and "i3.bz2" in s['name']]
            sRunInfo_sorted = sorted(sRunInfo, key=lambda k:['name'])
            #RunInfo = sRunInfo[0:3]

            
            for sr in sRunInfo:
            #
                nName = sr['name'].replace("Level2_","Level3_").replace("Test_","")
                #print nName
                
                #print "here"
                #print "checking records for L2 input %s"%sr['name']
                #SUBMITLOG.write("\n"checking records for L2 input %s"%sr['name'])
                
                nRecord = []
                nRecord = [d for d in dRunInfo if d['name']==nName]

                if len(nRecord)!=1:
                    # may just be a subrun that is good in L2 but bad in L3 e.g. really small L2 output so no L3 events
                    badRun = [d for d in dRunInfo if d['name']==sr['name']] # if no L3 output, check for L2 input record   
                    if badRun[0]['status'] == "BadRun" : continue           # skip subrun that has been declared bad
                    verified = notGood("no DB record (or more than 1) for in/output %s/%s dir. for run %s"%(sr['name'],nName,RunId),RunId)
                    continue
                nRecord = nRecord[0]
                OutDir = nRecord['path']
                if nRecord['status'] not in ("OK","BadRun"):
                    verified = notGood("DB record for in/output %s/%s dir. for run %s is %s"%(sr['name'],nName,RunId,nRecord['status']),RunId)
                    continue
                L3Out = os.path.join(nRecord['path'][5:],nRecord['name'])
                if not os.path.isfile(L3Out):
                    verified = notGood("out L3 file %s does not exist in  outdir. for run %s"%(L3Out,RunId),RunId)
        
            
            # in case last subrun was a badrun, pick last good subrun as nRecord
            nRecord = [d for d in dRunInfo if "Level3" in d['name'] and d['status']=="OK"]            
            nRecord = nRecord[-1]

            if MERGEHDF5:
                hdf5Files = []
                #hdf5Files = glob.glob(nRecord['path'][5:]+"/*%s*hdf5"%RunId)
                #hdf5Files = [h for h in hdf5Files if "Merged" not in h] # avoid previously meged hdf5 file if one exists
                
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
                    #hdf5Files = hdf5Files[0:2]
                    hdf5Files = " ".join(hdf5Files)
                    #print len(hdf5Files)
                    #print hdf5Files
                    
                    hdf5Out = nRecord['path'][5:]+"/Level3_IC86.%s_data_Run00%s_Merged.hdf5"%(SEASON,RunId)
                    #print hdf5Out
                
                    #if os.path.isfile(hdf5Out):
                    #    sub.call(["mv","%s"%hdf5Out,"%s"%os.path.join(os.path.dirname(hdf5Out),os.path.basename(hdf5Out).replace("Level3","Old_Level3"))])    
                            
                    mergeReturn = sub.call(["/data/user/i3filter/L3_Processing/RHEL_6.4_IC2012-L3_Muon_V2_NewCVMFS/./env-shell.sh",
                                        "python", "/data/user/i3filter/L3_Processing/RHEL_6.4_IC2012-L3_Muon_V2_NewCVMFS/hdfwriter/resources/scripts/merge.py",
                                        "%s"%hdf5Files, "-o %s"%hdf5Out])
                    
                    #print "mergeReturn: ",mergeReturn
                    if mergeReturn : verified = 0

                    dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","PERMANENT","%s","%s")\
                           on duplicate key update dataset_id="%s",queue_id="%s",name="%s",path="%s",type="PERMANENT",md5sum="%s",size="%s",transferstate="WAITING"  """% \
                                     (DDatasetId,nRecord['queue_id'],os.path.basename(hdf5Out),"file:"+os.path.dirname(hdf5Out)+"/",str(FileTools(hdf5Out).md5sum()),str(os.path.getsize(hdf5Out)),\
                                      DDatasetId,nRecord['queue_id'],os.path.basename(hdf5Out),"file:"+os.path.dirname(hdf5Out)+"/",str(FileTools(hdf5Out).md5sum()),str(os.path.getsize(hdf5Out))))
                    
                    
                    dbs4_.execute("""update i3filter.urlpath set transferstate="IGNORED" where dataset_id=%s and name like "%%%s%%hdf5%%" and name not like "%%Merged%%" """%(DDatasetId,RunId))

               
            if verified:
                print "Succesfully Verified processing for run %s \n==============="%RunId
                SUBMITLOG.write("\nSuccesfully Verified processing for run %s \n==============="%RunId)
            else:
                print "Failed Verification for run %s, see other logs \n==============="%RunId
                SUBMITLOG.write("\nFailed Verification for run %s, see other logs \n================"%RunId)
            
        except Exception,err:
            #raise Exception("Error: %s\n"%str(err))
            print "Error: %s\n"%str(err)
            print "skipping verification for %s, see previous error"%RunId

if __name__ == '__main__':

    params = GetParams()
    if not params["SDatasetId"] or not params["DDatasetId"] or not len(params["OUTDIR"]) or not len(params["SEASON"]):
        print """you must enter 'source' and 'destination' dataset_ids, the year/season and a main 'output directory' path for submission"""
        exit(1)



    try:
        SubmissionLogFile = "./L3_PostProcessing_Season-%s_DatasetId-%s_"%(params["SEASON"],params["DDatasetId"])+time.strftime("%Y_%m_%d-%H_%M_%S",time.localtime()) + ".log"
    except:
        SubmissionLogFile = "./L3_PostProcessing_Season-%s_DatasetId-%s_"%(params["SEASON"],params["DDatasetId"])

    
    SUBMITLOG = open(SubmissionLogFile,'w')
    
    #parser.add_option("-o", "--outputlog", default=SubmissionLogFile,
    #                                  dest="OUTPUTLOG_", help="submission log file, default is time-stamped file name with submission location as default")
    #

    SUBMITLOG.write("Production parameters:\n%s\n"%params)

    main(params,SUBMITLOG)
    
    SUBMITLOG.close()
