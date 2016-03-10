#!/usr/bin/env python

import os, sys
import subprocess as sub
import StringIO
import logging
import copy
import datetime
from optparse import OptionParser
from I3Tray import *
from icecube import icetray, dataclasses, dataio, phys_services,tpx, portia, paraboloid, common_variables

from icecube.filterscripts.offlineL2.Rehydration import Rehydration

import glob
import datetime
from dateutil.relativedelta import *

from icecube import I3Db

from RunTools import *
from FileTools import *


def ComputTenthOfNanosec(time_,time_frac):
    try:
        if time_frac is None : time_frac = 0
        return ((datetime.date(int(time_.year),int(time_.month),int(time_.day)) - \
                    datetime.date(int(time_.year),1,1)).days * 86400 + \
                    int(time_.hour) * 3600 + \
                    int(time_.minute)  * 60 + \
                    int(time_.second)) * 10000000000  + \
                    int(time_frac)
        
    except Exception, err:
        print "ComputTenthOfNanosec Error: " + str(err)
        exit(1)

def GetGoodRunTimes(dbs4_,RunNum,SnapshotId):
    
    try:

        GRLInfo = dbs4_.fetchall(""" select * from grl_snapshot_info g
                                     where g.run_id=%s and g.snapshot_id=%s
                                     """%(RunNum,SnapshotId),UseDict=True)
        
        print GRLInfo
        
        goodStartTime = dataclasses.I3Time()
        tNanoSec = ComputTenthOfNanosec(GRLInfo[0]['good_tstart'],GRLInfo[0]['good_tstart_frac'])
        goodStartTime = dataclasses.I3Time(int(GRLInfo[0]['good_tstart'].year),tNanoSec)
        
        goodEndTime = dataclasses.I3Time()
        tNanoSec = ComputTenthOfNanosec(GRLInfo[0]['good_tstop'],GRLInfo[0]['good_tstop_frac'])
        goodEndTime = dataclasses.I3Time(int(GRLInfo[0]['good_tstop'].year),tNanoSec)
        
        
        return goodStartTime,goodEndTime
        
    except Exception, err:
        print "GetGoodRunTimes Error: " + str(err)
        exit(1)


def SetGRLInfo(frame,ProductionVersion,SnapshotId,StartTime, EndTime):

    frame["OfflineProductionVersion"] = dataclasses.I3Double(ProductionVersion)
    frame["GRLSnapshotId"] = dataclasses.I3Double(SnapshotId)
    frame["GoodRunStartTime"] = StartTime
    frame["GoodRunEndTime"] = EndTime


def AdjustDSTime(frame,RunNum):
    # adjust DetectorStatus times when they are way off from the actual start time because of a bug
    
    
    import SQLClient_i3live as live
    m_live = live.MySQL()
    
    tmp = m_live.fetchall(""" SELECT r.tStart,r.tStop
                             FROM live.livedata_snapshotrun l
                             join live.livedata_run r on l.run_id=r.id
                             where r.runNumber=%s"""%(RunNum),UseDict=True)
    tmp = tmp[0]
    tStart = tmp['tStart']
    tStop = tmp['tStop']

    if abs((frame['I3DetectorStatus'].start_time.date_time - tStart).total_seconds()) > 100:
        print "DS Start Time Needed Adjustment"
        print "Original Start Time is: ",frame['I3DetectorStatus'].start_time.date_time
        print "Replaced with Start Time from i3live: ",tStart
        tNanoSec = ComputTenthOfNanosec(tStart,None)
        StartTime = dataclasses.I3Time(tStart.year,tNanoSec)
        frame['I3DetectorStatus'].start_time = StartTime
        
        
    if abs((frame['I3DetectorStatus'].end_time.date_time - tStop).total_seconds()) > 100:
        print "DS End Time Needed Adjustment"
        print "Original End Time is: ",frame['I3DetectorStatus'].end_time.date_time
        print "Replaced with End Time from i3live: ",tStop
        tNanoSec = ComputTenthOfNanosec(tStop,None)
        StopTime = dataclasses.I3Time(tStop.year,tNanoSec)
        frame['I3DetectorStatus'].end_time = StopTime
        

def MakeGCD(RunNum,FName,GCDName,ProductionVersion,SnapshotId,effectiveStartTime,effectiveEndTime):
    print RunNum,FName,GCDName,ProductionVersion,SnapshotId,effectiveStartTime,effectiveEndTime
    try:
        
        from icecube.BadDomList.BadDomsList_TraySegment import BadDomList as BDList
        from icecube.phys_services.spe_fit_injector import I3SPEFitInjector
        import traceback
          
        tray = I3Tray()
           
        tray.AddModule("I3Reader","readPFFiltFile", filename = FName)

        tray.AddModule("QConverter", "qify", WritePFrame=False)
        #tray.AddModule("QConverter", "qify")
          
        tray.AddSegment(I3Db.GCDSynth,"gcdsynth",
                        Host = '128.104.255.19',        # new external ip address for dbs2
                        #Host = 'dbs2.icecube.wisc.edu',
                        #Host = 'icedb.umons.ac.be',
                        Mjd = 54560 # any nominal value > PMTInfoIntroduction (54559, 2008/04/03)
                        #Database = 'I3OmDb'
                        )

        #spe_correction_file = os.path.expandvars("$I3_SRC") + "/filterscripts/resources/data/final-spe-fits-pole-run2015.json"
        spe_correction_file = os.path.expandvars("$I3_SRC") + "/filterscripts/resources/data/spe-fits-IC86-2015-05-11-2015-V1.json"
        tray.AddModule(I3SPEFitInjector, "fixspe", Filename = spe_correction_file)


        tray.AddModule(SetGRLInfo,"SetGRLInfo",
                        StartTime = effectiveStartTime,
                        EndTime = effectiveEndTime,
                        ProductionVersion = ProductionVersion,
                        SnapshotId = SnapshotId,
                        Streams=[icetray.I3Frame.DetectorStatus])

        tray.AddModule(AdjustDSTime,"AdjustDSTime",
                        RunNum = RunNum,
                        Streams=[icetray.I3Frame.DetectorStatus])

        tray.AddSegment(BDList,"baddomlist",DataType=True, run=RunNum)

        tray.AddModule('I3IceTopSanityChecks', 'IceTopSanityChecks',
            InputBadDOMList      = 'BadDomsList',
            BadDOMList           = 'IceTopBadDOMs',
            BadTankList          = 'IceTopBadTanks',
            #BadStationList       = 'IceTopBadStations',
            CheckGainCalibration = True,                      # Default
            CheckTimeCalibration = True                       # Default
            )


        tray.AddModule('I3Writer', 'GCDWriter',
                               FileName = GCDName, # ! Name of GCD output file
                               Streams = [ icetray.I3Frame.Geometry, # ! Only write the GCD frames
                                           icetray.I3Frame.Calibration,
                                           icetray.I3Frame.DetectorStatus ],
                               )

        tray.AddModule("TrashCan","trash")

        tray.Execute(1)
        #tray.Execute()

        tray.Finish()

        del tray
        return 0
        #return ""
    except Exception, err:
        print " MakeGCD Error: " + str(err)
        print Exception
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        del tray
        return 1
        #return str(err)


def AuditGCD(GCDName):   
    try:
        tray = I3Tray()
        tray.AddModule("I3Reader","readGCD", filename = GCDName)
        #tray.AddModule('I3GCDAuditor', 'GCDAuditor')#, MaximumParanoia=False)
        tray.AddModule('I3GCDAuditor', 'GCDAuditor', MaximumParanoia=True)
        
        tray.Execute()
        tray.Finish()
    
        del tray
        return 0

    except Exception, err:
        del tray
        print "AuditGCD Error: " + str(err)
        return 1

        
def Rehydrate(GCDName,FName,RunNum,RName):
    try:
        tray = I3Tray()

        infiles = [GCDName,FName]
        
        tray.AddModule( "I3Reader", "Reader")(
        ("Filenamelist", infiles) )
        
        tray.AddSegment(Rehydration, 'rehydrator',
                        doNotQify=False
                        )
        
        tray.AddModule('I3Writer', 'OutWriter',
                               FileName = RName,
                               Streams = [icetray.I3Frame.DAQ, icetray.I3Frame.Physics],
                               )
        
        tray.AddModule("TrashCan","trash")
            
        #tray.Execute(10000)
        tray.Execute()
        tray.Finish()
        
        del tray
        #return ""
    except Exception, err:
        del tray
        #return str(err)
        print "Rehydrate Error: " + str(err)

def BadDOMAudit(GCDName,RNames):
    try:
        tray = I3Tray()

        FList = [GCDName]
        FList.extend(RNames)

        print FList

        tray.AddModule('I3Reader', 'reader', FilenameList=FList)

        ##tray.AddModule('I3BadDOMAuditor', 'audit', BadDOMList="BadDomsListSLC", Pulses=['OfflinePulses', 'OfflineIceTopHLCTankPulses'])
        tray.AddModule('I3BadDOMAuditor', 'BadDOMAuditor',
                        BadDOMList="BadDomsListSLC",
                        Pulses=['InIcePulses','IceTopPulses'])

        tray.AddModule('TrashCan', 'can')

        tray.Execute()
        tray.Finish()

        del tray
        return 0
        #return ""

    except Exception, err:
        del tray
        #return str(err)
        #print str(err)
        print "BadDOMAudit Error: " + str(err)
        return 1


def main(RunNum, ProductionVersion, SnapshotId):

    ScratchSpace = "./"
    try:
        if os.access(os.environ["_CONDOR_SCRATCH_DIR"],os.W_OK): ScratchSpace = os.environ["_CONDOR_SCRATCH_DIR"]
    except:
        pass

    print "ScratchSpace: ",ScratchSpace

    OutName = "tmp_L2_%s.out"%os.sys.argv[1]
    OutName = os.path.join(ScratchSpace,OutName)

    icetray.logging.rotating_files(OutName)

    # attempt to determine run season/year
    try:
        Season = ""
        if int(RunNum)>=118175 and int(RunNum)<=120155 :
            Season="IC86.2011_"
        if int(RunNum)>=120156 and int(RunNum) <= 122275 :        
            Season="IC86.2012_"
        if int(RunNum)>=122276 :       
            Season="IC86.2013_"
        if int(RunNum)>=124702 :        # current running season, open ended for now. Another if statement will be added with a new season
            Season="IC86.2014_"
        # IC86_2015 24hr test runs (126289>=x<=126191) and IC86_2015 runs (>=126378)
        if int(RunNum)==126289 or int(RunNum)==126290 or int(RunNum)==126291 or int(RunNum)>=126378:    
            Season="IC86.2015_"
    except Exception, err:
        raise Exception("Error: %s "%str(err))

    # just for the IC86_2014 24hr test run, should be removed and previous lines changed for production
    #Season="IC86.2014_"

    try:
        import SQLClient_i3live as live
        m_live = live.MySQL()
        import SQLClient_dbs4 as dbs4
        dbs4_ = dbs4.MySQL()

    except Exception, err:
        raise Exception("Error: %s "%str(err))

    R = RunTools(RunNum)
    RunTimes = R.GetRunTimes()
    
    sDay = RunTimes['tStart']

    fDir = "/data/exp/IceCube/%s/filtered/PFFilt/%s%s"%(sDay.year,str(sDay.month).zfill(2),str(sDay.day).zfill(2))
    
    if not os.path.isdir(fDir):
        print "dir = %s that should contain input PFFilt files does not exist"%fDir
        exit(1)

    InFiles = R.GetRunFiles(RunTimes['tStart'],'P')
    
    if not len(InFiles):
        print "no input files for Run=%s in possible dirs %s"%(RunNum,fDir)
        exit (1)
       
    
    InFiles.sort()

    
    GCDDir = "/data/exp/IceCube/%s/filtered/level2/OfflinePreChecks/DataFiles/%s%s"%(sDay.year, str(sDay.month).zfill(2),str(sDay.day).zfill(2))
    #GCDDir = "."
    try:
        if not os.path.exists(GCDDir):
            os.mkdir(GCDDir)
    except Exception, err:
        print str(err)
    
    GCDDirAll = "/data/exp/IceCube/%s/filtered/level2/AllGCD/"%sDay.year
    GCDDirVerified = "/data/exp/IceCube/%s/filtered/level2/VerifiedGCD/"%sDay.year    
    #GCDDirAll = "."
    #GCDDirVerified = "."

    GCDName = os.path.join(GCDDir,"Level2_%sdata_Run00%s_%s_%s_GCD.i3.gz"%(Season,RunNum,ProductionVersion,SnapshotId))

    GCDLinkName = "Level2_%sdata_Run00%s_%s%s_%s_%s_GCD.i3.gz"%(Season,RunNum,str(sDay.month).zfill(2),str(sDay.day).zfill(2),ProductionVersion,SnapshotId)

    effectiveStartTime,effectiveStopTime = GetGoodRunTimes(dbs4_,RunNum,SnapshotId)

    print "\nGenerating GCD file for run %s"%RunNum
    #Gerr = MakeGCD(RunNum,InFiles[0],GCDName,ProductionVersion,SnapshotId,effectiveStartTime,effectiveStopTime)
    MakeGCD(RunNum,InFiles[0],GCDName,ProductionVersion,SnapshotId,effectiveStartTime,effectiveStopTime)
    
    #if len(Gerr):
    #    print "GCD Generation error: ", Gerr
    #else:
    #    print "GCD Generation completed without error"
    
    #print "==== END: Generating GCD file ====\n"
    print "\n==== Completed GCD generation attempt for run %s\n"%RunNum
    
    
    if os.path.isfile(GCDName) and os.path.getsize(GCDName)>0:
    
        os.system("ln -sf %s %s/%s"%(GCDName,GCDDirAll,GCDLinkName))
        
        print "\nAuditing GCD file for run %s"%RunNum
    
        #with to_file(stream=sys.stdout,file=OutName):
        AuditGCD(GCDName)
            
        with open(OutName,'r') as f:
            try:
                l = str(f.read())
                
                print "\n====Start GCDAudit Log===\n\n",l,"\n===End GCDAudit Log====\n"
                
                l = l.split("\n")

                #print "====== GCD Output before filtering ===="
                #print l
                #print "====== End GCD Output before filtering ===="
    

                h = [n for n in l if ("GCDAuditor" in n \
                                  and "ERROR" in n \
                                  and "OMOMKey(19,60,0)" not in n)
                                  #and "OMOMKey" in n \         # not needed
                                  #and "SPE threshold for OMOMKey(35,62,0)" not in n
                                ]       
                                  
                print "====== GCD Output after filtering ===="
                print h
                print "====== End GCD Output after filtering ===="
                
                if not len(h):   
                    
                    print "GCD Audit for %s OK"%GCDName
        
                    #dbs4_.execute("""insert into i3filter.pre_processing_checks (run_id,GCDCheck) values(%s,1) on duplicate key update GCDCheck=1 """%(RunNum))
                    
                    dbs4_.execute("""update i3filter.grl_snapshot_info set GCDCheck=1
                                  where run_id=%s and snapshot_id=%s """%(RunNum,SnapshotId))
                    
                    #print "Updating pre_processing table in DB"
        
                else:
                    print "GCD audit failed so will NOT proceed with BadDOMs audit. GCDAudit error: \n======\n", l,"\n======"
                    exit (1)
                    #f.close()
            except Exception, err:
                print str(err)
                exit (1)

        print "==== END: Auditing GCD file ====\n"
    
        
        ###### Start: Bad DOMs Auditing ########
        
        def parseAuditLogs(RName,AuditFile):
            with open(AuditFile,'r') as f:   
                try:
                    d = str(f.read())
        
                    print "\n====Start BadDoms Log===\n",d,"\n===End BadDoms Log====\n"
                    
                    d = d.split("\n")
        
                    b = [n for n in d if "BadDOMAuditor" in n \
                                and "ERROR" in n \
                                and "OMOMKey" in n]
        

                    if not len(b):
                        return 1
                    
                    else:
                        print "BadDOMs auditing errors for input file %s"%RName
                        print "BadDoms Audit Error:\n=====\n",d,"\n======="
                        return 0   
                except Exception, err:
                    print str(err)
            
        
        print "\nStart: BadDOMs Auditing for GCD file for run %s"%RunNum
        try:
            F = 0
            # try auditing first subrun
            RName = ("R_"+os.path.basename(InFiles[0])).replace(".tar.bz2",".i3.bz2")
            RName = os.path.join(ScratchSpace,RName)
            Rehydrate(GCDName,InFiles[0],RunNum,RName)
            
            print "First infile for BadDOMAudit check: %s"%InFiles[0]
            BadDOMAudit(GCDName,[RName])
        
            checkFirst = parseAuditLogs(RName,OutName)
            
            if checkFirst:
                # this further check is necessary in case a DOM drops out during a run, this ensures that all DOMs
                # are present all through data taking
                
                # pick 2nd to last sub-run if you have more than 2 sub-runs, otherwise just pick last sub-run
                # prefer to use 2nd to last sub-run because many last sub-runs are really short resulting in false alarm
                # any run with < 3 subruns is probably deemed bad because of the run length requirement
                if len(InFiles)>2:
                    InFile_ = InFiles[-2]
                    RName = ("R_"+os.path.basename(InFile_)).replace(".tar.bz2",".i3.bz2")
                else:
                    InFile_ = InFiles[-1]
                    RName = ("R_"+os.path.basename(InFile_)).replace(".tar.bz2",".i3.bz2")
                
                
                RName = os.path.join(ScratchSpace,RName)
                Rehydrate(GCDName,InFile_,RunNum,RName)
            
                print "Second/Last infile for BadDOMAudit check: %s"%InFile_
                BadDOMAudit(GCDName,[RName])
        
                checkOther = parseAuditLogs(RName,OutName)
                
                if checkOther:
                    F=1
                        
                    os.system("ln -sf %s %s/%s"%(GCDName,GCDDirVerified,GCDLinkName))
                    
                    dbs4_.execute("""update i3filter.grl_snapshot_info set BadDOMsCheck=1
                                  where run_id=%s and snapshot_id=%s """%(RunNum,SnapshotId))
                    
                        
                    print "BadDoms Audit for %s OK"%GCDName 
                else:
                    print "BadDoms Audit for %s has PROBLEMS"%GCDName
                        
            #print "BadDom Audit return value: ",F
                    
                
        except Exception, err:
                print str(err)
                exit (1)
        
        print "\nEnd: BadDOMs Auditing for GCD file for run %s"%RunNum
        
    
    
    
if __name__ == '__main__':
    if len(os.sys.argv) != 4:
        print "Run number, processing Version and snapshotId must be supplied"
        exit(1)

    try:
        int(os.sys.argv[1])
        int(os.sys.argv[2])
        int(os.sys.argv[3])
    except Exception, err:
        print " *** Input run number, processing Version and snapshotId arguments have to be int e.g. 123456 0 9 ***"
        raise Exception("Error: %s "%str(err))   
    
    print "Processing run %s with ProcessingVersion:%s SnapshotId:%s"%(os.sys.argv[1],os.sys.argv[2],os.sys.argv[3])
    
    main(os.sys.argv[1],int(os.sys.argv[2]),int(os.sys.argv[3]))
