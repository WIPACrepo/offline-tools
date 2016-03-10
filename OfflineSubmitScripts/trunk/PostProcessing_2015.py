#!/usr/bin/env python

import os, sys
import subprocess as sub
import time
import datetime
import argparse

from RunTools import *
from FileTools import *
from DbTools import *

from GoodRuntimeAdjust_2015 import main as GoodRuntimeAdjust

try:
    import SQLClient_i3live as live
    m_live = live.MySQL()
    
    import SQLClient_dbs4 as dbs4
    dbs4_ = dbs4.MySQL()
    
    import SQLClient_dbs2 as dbs2
    dbs2_ = dbs2.MySQL()
    
except Exception, err:
    raise Exception("Error: %s "%str(err))

def PrintVerboseDifference(FList1,FList2):
    tmp_diff = list(set(FList1) - set(FList2))
    if len(tmp_diff):
        tmp_diff.sort()
        print "entries on disk but not in db"
        for t_d in tmp_diff:
            print t_d
    tmp_diff = list(set(FList2) - set(FList1))
    if len(tmp_diff):
        tmp_diff.sort()
        print "entries in db but not on disk"
        for t_d in tmp_diff:
            print t_d

def CheckFiles(r):
    try:
        if not r['GCDCheck'] or not r['BadDOMsCheck']:
            print "GCDCheck or BadDOMsCheck failed for run=%s, production_version%s"%\
                   (str(r['run_id']),str(r['production_version']))
            return 1
        
        R = RunTools(r['run_id'])
        InFiles = R.GetRunFiles(r['tStart'],'P')
        OutFiles = R.GetRunFiles(r['tStart'],'L')
    
        
        ProdVersion = "%s_%s/"%(str(r['run_id']),str(r['production_version']))
        #ProdVersion = "%s_%s_%s"%(str(r['run_id']),str(r['production_version']),str(r['snapshot_id']))  
        
        Files2Check = []
        
        # check for multiple GCD files in out Dir, usually results from re-processing
        #print [f for f in OutFiles if "GCD" in f and str(r['run_id'])+"_"+]
        #if len([f for f in OutFiles if "GCD" in f ]) !=1 :
        #    print "Either None or more than 1 GCD file in output dir for run=%s"%str(r['run_id'])
        #    return 1
        
        GCDName = [f for f in OutFiles if "GCD" in f and ProdVersion in f]
        #GCDName = [f for f in OutFiles if "GCD" in f ]

        if len(GCDName)!=1:
            print "Either None or more than 1 GCD file in output dir for run=%s"%str(r['run_id'])
            return 1
        
        GCDName = GCDName[0]
        #GCDName = [f for f in OutFiles if "GCD" in f and ProdVersion in f][0]
        GCDName = os.path.join('/data/exp/IceCube/%s/filtered/level2/VerifiedGCD'\
                           %r['tStart'].year,os.path.basename(GCDName))

        if not os.path.isfile(GCDName):
            print "No Verified GCD file for run=%s, production_version%s"%\
                   (str(r['run_id']),str(r['production_version']))
            return 1
        
        Files2Check.append(GCDName)    
        
        L2Files = [f for f in OutFiles if "GCD" not in f \
                       and "txt" not in f and "root" not in f\
                       and "EHE" not in f and "IT" not in f \
                       and "log" not in f
                       and ProdVersion in f]  
        L2Files.sort()
       

        if len(InFiles) != len(L2Files):
            print "No. of Input and Output files don't match for run=%s, production_version=%s"%\
                   (str(r['run_id']),str(r['production_version']))
            return 1

        for p in InFiles:
            l = os.path.join(os.path.dirname(L2Files[0]),os.path.basename(p).replace\
                 ("PFFilt_PhysicsFiltering","Level2_IC86.2015_data").replace\
                 (".tar",".i3").replace\
                 ("Subrun00000000_","Subrun"))
    

            if not os.path.isfile(l):
                print "At least one output file %s does not exist for input file %s"%(l,p)
                return 1

            Files2Check.append(p)
            Files2Check.append(l)
    
        Files2CheckS = """'""" + """','""".join(Files2Check) + """'"""
        
        FilesInDb = dbs4_.fetchall("""SELECT distinct name,concat(substring(u.path,6),"/",u.name)
                                      from i3filter.urlpath u
                                     where u.dataset_id=1883 and
                                     concat(substring(u.path,6),"/",u.name) in (%s) or \
                                     concat(substring(u.path,6),u.name) in (%s) """%\
                                     (Files2CheckS,Files2CheckS))
        
        FilesInDb = [f[1].replace('//','/') for f in FilesInDb]
        

        if len(Files2Check) != len(FilesInDb):
            print "Some file records don't exist for run=%s, production_version=%s"%\
                   (str(r['run_id']),str(r['production_version']))
            PrintVerboseDifference(Files2Check,FilesInDb) 
            return 1
        
        # make symlink to latest output dir
        baseDir = "/data/exp/IceCube/%s/filtered/level2/%s%s"%\
                  (r['tStart'].year,str(r['tStart'].month).zfill(2),str(r['tStart'].day).zfill(2))
        OutDirs = [g.split("_")[-1] for g in glob.glob(os.path.join(baseDir,"Run00%s_*"%r['run_id']))]
        OutDirs.sort(key=int)
        LatestDir = os.path.join(baseDir,"Run00%s_%s"%(r['run_id'],OutDirs[-1]))
        
        LinkDir = os.path.join(baseDir,"Run00%s"%r['run_id'])
        
        if os.path.lexists(LinkDir):
            sub.call(["rm","%s"%LinkDir])
        ln_ret = sub.call(["ln","-s","%s"%LatestDir,"%s"%LinkDir])
        
        if ln_ret:
            print "Could not make symlink to latest production for run=%s"%\
                   (str(r['run_id']))
            return 1
        
        
        return 0
        
    except Exception, err:
        #raise Exception("Run=%s Error: %s"%(r,str(err)))
        print "FileChecks Error: %s \n for Run=%s"%(str(err),r['run_id'])
        return 1


def MakeTarGapsTxtFile(StartTime,RunId):
    
    try:
        
        OutDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/Run00%s/"%\
                     (StartTime.year,str(StartTime.month).zfill(2),\
                      str(StartTime.day).zfill(2),RunId)
        
        OutTar = os.path.join(OutDir,"Run00"+str(RunId)+"_GapsTxt.tar")
        
        
        gapsFiles = glob.glob(os.path.join(OutDir,"*_gaps.txt"))
        gapsFiles = [os.path.basename(g) for g in gapsFiles]
        gapsFiles.sort()
        sub.check_call(["tar","cf",OutTar,"-C",OutDir,gapsFiles[0]])
        for g in gapsFiles[1:]:
            sub.check_call(["tar","rf",OutTar,"-C",OutDir,g])
        

        
        maxQId = dbs4_.fetchall("""SELECT max(u.queue_id) FROM i3filter.urlpath u join i3filter.run r on u.queue_id=r.queue_id
                     where r.dataset_id=1883 and u.dataset_id=1883 and r.run_id=%s"""%RunId)
        
        
    
        dbs4_.execute(""" update i3filter.urlpath u join i3filter.run r on u.queue_id=r.queue_id set u.transferstate="IGNORED"
                  where r.dataset_id=1883 and u.dataset_id=1883 and r.run_id=%s and u.name like "%%_gaps.txt" """%RunId)
        
        dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","PERMANENT","%s","%s")\
                       on duplicate key update dataset_id="%s",queue_id="%s",name="%s",path="%s",type="PERMANENT",md5sum="%s",size="%s",transferstate="WAITING"  """% \
                                 ('1883',str(maxQId[0][0]),os.path.basename(OutTar),"file:"+os.path.dirname(OutTar)+"/",str(FileTools(OutTar).md5sum()),str(os.path.getsize(OutTar)),\
                                  '1883',str(maxQId[0][0]),os.path.basename(OutTar),"file:"+os.path.dirname(OutTar)+"/",str(FileTools(OutTar).md5sum()),str(os.path.getsize(OutTar))))
    
        return 0
    
    except Exception, err:
        print "MakeTarGapsTxtFile Error: %s \nfor Run=%s"\
               %(str(err),RunId)
        return 1


def MakeRunInfoFile():
    try:
            
        RunInfo = dbs4_.fetchall("""SELECT * FROM i3filter.grl_snapshot_info g
                                     join i3filter.run_info_summary r on r.run_id=g.run_id
                                     join i3filter.run jr on jr.run_id=r.run_id
                                     where jr.dataset_id=1883 and (g.good_i3 or g.good_it) and g.submitted
                                     group by jr.run_id
                                     order by g.run_id,g.production_version""",UseDict=True)
    
        RunInfoDict = {}
        for r in RunInfo:
            RunInfoDict[r['run_id']] = r
        keys_ = RunInfoDict.keys()
        keys_.sort()
        
        ProductionYear = str(RunInfoDict[keys_[0]]['tStart'].year)
        
        
        LatestProductionVersion = str(RunInfoDict[keys_[-1]]['production_version'])
        
        RunInfoFile = "/data/exp/IceCube/%s/filtered/level2/RunInfo/IC86_%s_GoodRunInfo_%s_"%(ProductionYear,ProductionYear,LatestProductionVersion)+time.strftime("%Y_%m_%d-%H_%M_%S",time.localtime()) + ".txt"
    
        RunInfoFileV = "/data/exp/IceCube/%s/filtered/level2/RunInfo/IC86_%s_GoodRunInfo_%s_Versioned_"%(ProductionYear,ProductionYear,LatestProductionVersion)+time.strftime("%Y_%m_%d-%H_%M_%S",time.localtime()) + ".txt"
    
        RI_File = open(RunInfoFile,'w')
        RI_FileV = open(RunInfoFileV,'w')
        
        #RI_File.write("RunNum  Good_i3  Good_it  LiveTime(s)            OutDir")
        #RI_FileV.write("RunNum  Good_i3  Good_it  LiveTime(s)            OutDir")    
        
        
        #RI_File.write("RunNum  Good_i3  Good_it  LiveTime(s) ActiveStrings   ActiveDoms        OutDir                                                  Comment(s)")
        #RI_File.write("\n         (1=good 0=bad)  ")
        #
        #RI_FileV.write("RunNum  Good_i3  Good_it  LiveTime(s) ActiveStrings   ActiveDoms       OutDir                                                 Comments(s)")
        #RI_FileV.write("\n         (1=good 0=bad)  ")
        #
        RI_File.write("RunNum  Good_i3  Good_it  LiveTime(s) ActiveStrings   ActiveDoms     ActiveInIce        OutDir                                                  Comment(s)")
        RI_File.write("\n         (1=good 0=bad)  ")
       
        RI_FileV.write("RunNum  Good_i3  Good_it  LiveTime(s) ActiveStrings   ActiveDoms        ActiveInIce       OutDir                                                 Comments(s)")
        RI_FileV.write("\n         (1=good 0=bad)  ")
        
        
        for k in keys_:

            if not RunInfoDict[k]['validated']:
                RI_File.write("\n%s  **Incomplete Processing or Not Validated**"%k)
                RI_FileV.write("\n%s  **Incomplete Processing or Not Validated**"%k)
                continue
       
            StartTime = RunInfoDict[k]['tStart'] 

            LT = RunInfoDict[k]['good_tstop'] - RunInfoDict[k]['good_tstart']
            LiveTime = (LT.microseconds + (LT.seconds + LT.days *24 *3600) * 10**6) / 10**6
            
            
            Comments = ""
            #if int(k) >= 123738 and int(k) <= 124199 : Comments = "Inaccurate Icetop Reconstructions"
            if int(k) >= 126289 and int(k) <= 126291 : Comments = "IC86_2015 24hr test run"
            #if int(k) == 125261 :
            #    Comments = "111 sec gap due to missing subrun 32"
            #    LiveTime = LiveTime - 111
            #if int(k) == 125553 :
            #    Comments = "32 sec gap in the middle of the run between sub-runs 158/159 "
            #    LiveTime = LiveTime - 32
            
            
            ActiveStrings = "  "
            if RunInfoDict[k]['ActiveStrings'] is not None :  ActiveStrings = str(RunInfoDict[k]['ActiveStrings'])
            ActiveDOMs = "    "
            if RunInfoDict[k]['ActiveDOMs'] is not None :  ActiveDOMs = str(RunInfoDict[k]['ActiveDOMs'])
            ActiveInIceDOMs = "    "
            if RunInfoDict[k]['ActiveInIceDOMs'] is not None :  ActiveInIceDOMs = str(RunInfoDict[k]['ActiveInIceDOMs'])

            OutDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/Run00%s/"%\
                     (StartTime.year,str(StartTime.month).zfill(2),\
                      str(StartTime.day).zfill(2),k)
        
            
            RI_File.write("\n%s     %s        %s        %s           %s          %s         %s          %s    %s"%\
                        (k,RunInfoDict[k]['good_i3'],RunInfoDict[k]['good_it'],\
                         LiveTime,ActiveStrings,ActiveDOMs,ActiveInIceDOMs, OutDir, Comments))
        
            RI_FileV.write("\n%s     %s        %s        %s           %s          %s            %s          %s   %s"%\
                        (k,RunInfoDict[k]['good_i3'],RunInfoDict[k]['good_it'],\
                         LiveTime,ActiveStrings,ActiveDOMs,ActiveInIceDOMs, os.path.realpath(OutDir), Comments)) 
            
        
        RI_File.close()
        RI_FileV.close()
        
        LatestGoodRunInfo = glob.glob("/data/exp/IceCube/%s/filtered/level2/RunInfo/IC86_%s_GoodRunInfo_%s_2*"%(ProductionYear,ProductionYear,LatestProductionVersion))
        LatestGoodRunInfo.sort(key=lambda x: os.path.getmtime(x))
        LatestGoodRunInfo = LatestGoodRunInfo[-1]
        if os.path.lexists("/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo.txt"%(ProductionYear,ProductionYear)):
            sub.call(["rm","/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo.txt"%(ProductionYear,ProductionYear)])
        sub.call(["ln","-s","%s"%LatestGoodRunInfo,"/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo.txt"%(ProductionYear,ProductionYear)])
           
        LatestGoodRunInfoV = glob.glob("/data/exp/IceCube/%s/filtered/level2/RunInfo/IC86_%s_GoodRunInfo_%s_Versioned*"%(ProductionYear,ProductionYear,LatestProductionVersion))
        LatestGoodRunInfoV.sort(key=lambda x: os.path.getmtime(x))
        LatestGoodRunInfoV = LatestGoodRunInfoV[-1]
        if os.path.lexists("/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo_Versioned.txt"%(ProductionYear,ProductionYear)):
            sub.call(["rm","/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo_Versioned.txt"%(ProductionYear,ProductionYear)])
        sub.call(["ln","-s","%s"%LatestGoodRunInfoV,"/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo_Versioned.txt"%(ProductionYear,ProductionYear)])
        
        
    except Exception, err:
        print "MakeRunInfoFile Error: %s "%(str(err))


if __name__ == '__main__':


    parser = argparse.ArgumentParser(description="Post process runs after they have been processed by iceprod.")
    parser.add_argument('-r',nargs="?", help="run to postprocess",dest="run",type=int)
    args = parser.parse_args()
    if args.run is not None:
        RunInfo = dbs4_.fetchall("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info g
                                  join i3filter.run_info_summary r on r.run_id=g.run_id
                                  where g.submitted and (g.good_i3 or g.good_it or g.run_id in (126289,126290,126291)) and not validated and g.run_id = %i
                                  order by g.run_id""" %args.run,UseDict=True)
    else: 
        RunInfo = dbs4_.fetchall("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info g
                                 join i3filter.run_info_summary r on r.run_id=g.run_id
                                 where g.submitted and (g.good_i3 or g.good_it or g.run_id in (126289,126290,126291)) and not validated
                                 order by g.run_id""",UseDict=True)

    
#    #RunInfo = RunInfo[0:2]
#    RunInfo = [r for r in RunInfo if r['run_id']==122544 or r['run_id']==122545 ]
#    RunInfo = [r for r in RunInfo if r['run_id']==122545]
    for r in RunInfo:
        
        #if r['validated']: continue
        #print r
        
        #if r['run_id']!=126379: continue
        
        #if r['run_id']!=124702: continue
        #if r['run_id']!=125965: continue
        #if r['snapshot_id']!=76: continue
    
        print "\n======= Checking ",r['run_id'],str(r['production_version'])," ===="
    
        try:
            if DbTools(r['run_id'],1883).AllOk():
                print """Processing of Run=%s, production_version=%s
                         may not be complete ... skipping"""\
                        %(r['run_id'],str(r['production_version']))
                continue    
        except Exception, err:
            print "AllOk Error: %s \nfor Run=%s, production_version=%s"\
                    %(str(err),r['run_id'],str(r['production_version']))
            continue
         
        print "Running post processing checks for run: ",r['run_id']
        
        # check i/o files in data warehouse and Db
        print "    --Checking Files in Data warehouse and database records ..."
        if CheckFiles(r):
            print "FilesCheck failed: \nfor Run=%s, production_version=%s"\
                    %(r['run_id'],str(r['production_version']))
            continue
        print "              .... passed"
            
    
        ## delete/trim files when Good start/stop differ from Run start/stop
        print "    --Attempting to make adjustments to output Files to ensure all events fall within GoodRun start/stop time ..."
        try:
            GoodRuntimeAdjust(r['run_id'],r['production_version'])
            #sub.check_call(["python","/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/GoodRuntimeAdjust_2015.py",str(r['run_id']),str(r['production_version'])])
        except Exception, err:
                print "TrimFiles Error: %s \nfor Run=%s, production_version=%s"\
                    %(str(err),r['run_id'],str(r['production_version']))
                continue
                #raise Exception("Trim Files Error: %s "%str(err))
        print "              .... passed"
        
        
        print "    --Attempting to tar _gaps.txt files ..."
        if (MakeTarGapsTxtFile(r['tStart'],r['run_id'])):
            print "MakeTarGapsTxtFile failed: \nfor Run=%s, production_version=%s"\
                    %(r['run_id'],str(r['production_version']))
            continue
        print "              .... passed"
        
        
        print "    --Attempting to collect Active Strings/DOMs information from verified GCD file ..."
        R = RunTools(r['run_id'])
        #R.GetActiveStringsAndDoms(r['tStart'],UpdateDB=True)
        R.GetActiveStringsAndDoms(2015,UpdateDB=True)
        
             
        try:
            dbs4_.execute("""update i3filter.grl_snapshot_info 
                             set validated=1
                             where run_id=%s and production_version=%s"""%\
                         (r['run_id'],str(r['production_version'])))           
        except Exception, err:
            print "DB Validation update Error: %s \nfor Run=%s, production_version=%s"\
                    %(str(err),r['run_id'],str(r['production_version']))
            continue
        
        print "Successful validation for Run=%s, production_version=%s, all checks passed"\
                    %(r['run_id'],str(r['production_version']))
       
        
        
        print "======= End Checking ",r['run_id'],str(r['production_version'])," ====\n"    

    MakeRunInfoFile() 
