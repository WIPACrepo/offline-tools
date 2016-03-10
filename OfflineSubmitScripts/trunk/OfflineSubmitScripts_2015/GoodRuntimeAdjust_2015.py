#!/usr/bin/env python

import os, sys
import StringIO
import logging
from optparse import OptionParser
from dateutil.parser import parse
import glob
import datetime
import json
from dateutil.relativedelta import *
from math import *


try:
    from icecube.icetray import *
    from I3Tray import *
    #from icecube import icetray, dataclasses, dataio, phys_services,tpx, dst, linefit, I3Db
    from icecube import dataclasses, dataio
except Exception,err:
    print "\n*** Make sure I3Tray enviroment is enabled ...\n"
    raise Exception("Error: %s\n"%str(err))


from RunTools import *
from FileTools import *

#import MySQLdb
#sys.path.append("/net/user/i3filter/SQLServers_n_Clients/npx4/")

try:
    import SQLClient_i3live as live
    m_live = live.MySQL()
    
    import SQLClient_dbs4 as dbs4
    dbs4_ = dbs4.MySQL()
    
    import SQLClient_dbs2 as dbs2
    dbs2_ = dbs2.MySQL()
    
except Exception, err:
    raise Exception("Error: %s "%str(err))


class TrimFileClass(I3PacketModule):
   def __init__(self, context):
       I3PacketModule.__init__(self, context, icetray.I3Frame.DAQ)
       self.AddOutBox('OutBox')

   def Configure(self):
       pass
   def FramePacket(self, frames):
        #print frames[0]['I3EventHeader'].start_time<=GoodEnd
        
        #print frames[0]['I3EventHeader'].start_time>=GoodStart and \
        #        frames[0]['I3EventHeader'].end_time>=GoodStart and \
        #        frames[0]['I3EventHeader'].start_time<=GoodEnd and \
        #        frames[0]['I3EventHeader'].end_time<=GoodEnd and\
        #        icetray.I3Int(len(frames) - 1)>0, frames[0]['I3EventHeader'].start_time,GoodStart,GoodEnd
        
        if frames[0]['I3EventHeader'].start_time>=GoodStart and \
                frames[0]['I3EventHeader'].end_time>=GoodStart and \
                frames[0]['I3EventHeader'].start_time<=GoodEnd and \
                frames[0]['I3EventHeader'].end_time<=GoodEnd and\
                icetray.I3Int(len(frames) - 1)>0 :                       # only needed to take care of previously mis-trimmed files
                                                                        #   with "barren" Q frames
                    for fr in frames:
                        self.PushFrame(fr)


def ComputeTenthOfNanosec(time_,time_frac):
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


def GetSubRunStartStop(FileName,getStop=0):
    
    try:
        # attempt to get subrun start and stop times from gaps file ... cheaper
        gaps_file = str(FileName).replace(".i3.bz2","_gaps.txt")
        if os.path.isfile(gaps_file):
            with open(gaps_file,"r") as g:
                lines_ = g.readlines()
                f_ = [f for f in lines_ if "First" in f]
                fTenthofNanoSeconds = long(f_[0].split()[-1])
                fYear = int(f_[0].split()[-2])
                l_ = [l for l in lines_ if "Last" in l]
                lTenthofNanoSeconds = long(l_[0].split()[-1])
                lYear = int(l_[0].split()[-2])

            start_ = dataclasses.I3Time(fYear,fTenthofNanoSeconds)
            stop_ = dataclasses.I3Time(lYear,lTenthofNanoSeconds)
            
            if getStop: return start_,stop_
            return start_
    except:
        pass
        
    # try getting times from .i3 files if attempt from .gaps.txt file fails   
    start_ = (dataio.I3File(FileName)).pop_frame()['I3EventHeader'].start_time
    
    if getStop:
        try:
            i3file = dataio.I3File(FileName)
            while True: frame = i3file.next()
        except:
            pass
    
        stop_ = frame['I3EventHeader'].start_time
        
        return start_,stop_

    return start_
    

def GetGoodSubruns(OutFiles,GoodStartTime,GoodStopTime,ProdVersion):
    try:
        
        L2Files = [f for f in OutFiles if "GCD" not in f \
                   and "txt" not in f and "root" not in f\
                   and "EHE" not in f and "IT" not in f \
                   and "log" not in f
                   and ProdVersion in f]
        

        L2Files.sort()
        
        firstGood = L2Files[0]
        for f in L2Files:
            start_,end_ = GetSubRunStartStop(f,getStop=1)
            if start_<= GoodStartTime and end_>= GoodStartTime:
                firstGood = f
                break

        lastGood = L2Files[-1]
        for f in reversed(L2Files):
            start_,end_ = GetSubRunStartStop(f,getStop=1)
            if start_<= GoodStopTime and end_>= GoodStopTime:
                lastGood = f
                break

        return firstGood,lastGood,L2Files
    
    except Exception,err:
        raise Exception("GetGoodSubruns Error: %s\n"%str(err))


def RemoveBadSubRuns(L2Files,firstGood,lastGood,CleanDB = 0):
    try:
        
        print "Removing subruns not within good run range ...."
        
        L2Files.sort()
        
        firstGood_ = L2Files.index(firstGood)
        lastGood_ = L2Files.index(lastGood)
    
        Goodi3Files = L2Files[firstGood_:lastGood_+1]
        Badi3Files = [f for f in L2Files if f not in Goodi3Files]
    
        if not len(Badi3Files):
            print "No complete subruns to be removed ..."
            return
    
        #if len(Badi3Files):
        ToBeRemovedS = ""
        ToBeRemovedL = []
        
        # get <list> of files to be removed, used for data warehouse cleanup
        for b in Badi3Files:
            ToBeRemovedL.extend(glob.glob(b.replace(".i3.bz2","*")))
            ToBeRemovedL.sort()
    
        if not os.path.isdir(os.path.join(os.path.dirname(ToBeRemovedL[0]),"Bad_NotWithinGoodRunRange")):
            os.mkdir(os.path.join(os.path.dirname(ToBeRemovedL[0]),"Bad_NotWithinGoodRunRange"))
        
        BadDirOutput = os.path.join(os.path.dirname(ToBeRemovedL[0]),"Bad_NotWithinGoodRunRange")
         
        for r in ToBeRemovedL:
            BadName = os.path.join(os.path.dirname(r),BadDirOutput,os.path.basename(r).replace("Level2","BadDontUse_Level2"))
            print "moving %s %s"%(r,BadName)
            os.system("mv %s %s"%(r,BadName))
    
        if CleanDB:
            # get <str> version of list to be used in DB cleanup
            ToBeRemovedS = """'""" + """','""".join(ToBeRemovedL) + """'"""

            dbs4_.execute("""update i3filter.urlpath u
                     join i3filter.job j on u.queue_id=j.queue_id
                     set u.transferstate="DELETED",j.status="BadRun"
                     where u.dataset_id=%s and j.dataset_id=%s
                     and concat(substring(u.path,6),"/",u.name) in (%s)"""%("1883","1883",ToBeRemovedS))
                
         
    except Exception,err:
        raise Exception("RemoveBadSubruns Error: %s\n"%str(err))           




def TrimFile(InFile,GoodStart,GoodEnd):
    
    InFiles = [f for f in glob.glob(InFile.replace(".i3.bz2","*")) if ".i3" in f]
        
    for InFile_ in InFiles :
        print InFile_
    
        if (os.path.getsize(InFile_)) <= 14:
                print "skipping file=%s because it seems empty"%InFile_
                continue
    
        TrimmedFile = "Trimmed_"+os.path.basename(InFile_)
        
        tray = I3Tray()
        
        tray.AddModule("I3Reader","readL2File", filename = InFile_)
        
        #print L2File,GoodStart,GoodEnd
        tray.AddModule(TrimFileClass, 'Trim')
        
        #print "Trimmed_"+os.path.basename(L2File)
        
        tray.AddModule('I3Writer', 'FileWriter',
                        FileName = TrimmedFile,
                        Streams = [ I3Frame.DAQ,
                                    I3Frame.Physics]
                    )
        
        tray.AddModule("TrashCan","trash")
           
        #tray.Execute(100000)
        tray.Execute()
        
        tray.Finish()
        
        print "moving %s to %s"%(TrimmedFile,InFile_)
        os.system("mv -f %s %s"%(TrimmedFile,InFile_))
        
        
        dbs4_.execute("""update i3filter.urlpath u
                         set md5sum="%s", size="%s", transferstate="WAITING"
                         where u.dataset_id=%s 
                         and concat(substring(u.path,6),"/",u.name) = "%s" """%\
                        (str(FileTools(InFile_).md5sum()),str(os.path.getsize(InFile_)),"1883",InFile_))
        
        # re-write gaps.txt file using new main .i3 file
        if InFile_ == InFile:
            TFile = dataio.I3File(InFile_)
            FirstEvent = TFile.pop_frame()['I3EventHeader']
            while TFile.more(): LastEvent = TFile.pop_frame()['I3EventHeader']
            TFile.close()
            GFile = InFile.replace(".i3.bz2","_gaps.txt")
            if os.path.isfile(GFile):
                UpdatedGFile = os.path.join(os.path.dirname(GFile),'Updated_'+os.path.basename(GFile))
                with open(UpdatedGFile,"w") as u:
                    u.write('Run: %s\n'%FirstEvent.run_id)
                    u.write('First Event of File: %s %s %s\n'%(FirstEvent.event_id,\
                                                             FirstEvent.start_time.utc_year,\
                                                             FirstEvent.start_time.utc_daq_time)
                            )
                    u.write('Last Event of File: %s %s %s\n'%(LastEvent.event_id,\
                                                             LastEvent.end_time.utc_year,\
                                                             LastEvent.end_time.utc_daq_time)
                            )
                    u.write("File Livetime: %s\n"%str((LastEvent.end_time - FirstEvent.start_time)/1e9))
                        
                        
                print "moving %s to %s"%(UpdatedGFile,GFile)
                os.system("mv -f %s %s"%(UpdatedGFile,GFile))
                        
            
                dbs4_.execute("""update i3filter.urlpath u
                                set md5sum="%s", size="%s"
                                where u.dataset_id=%s 
                                and concat(substring(u.path,6),"/",u.name) = "%s" """%\
                                (str(FileTools(GFile).md5sum()),str(os.path.getsize(GFile)),"1883",GFile))




if __name__ == '__main__':

    if len(os.sys.argv) != 3:
        print "Run number  and production version must be supplied"
        exit(1)

    try:
        RunNum = int(os.sys.argv[1])
        ProductionVersion = int(os.sys.argv[2])
    except Exception, err:
        print " *** Input run number argument and production version have to be int e.g. 123456 ***"
        raise Exception("Error: %s "%str(err))   



    try:
 
        RunInfo = dbs4_.fetchall("""SELECT * FROM i3filter.grl_snapshot_info g
                                 join i3filter.run_info_summary r on r.run_id=g.run_id
                                 where g.run_id=%s and production_version=%s"""%\
                                (RunNum,ProductionVersion),UseDict=True)
        
        if not len(RunInfo):
            print "no DB records for %s ..... exiting"%RunNum
            exit(1)
        
        RunInfo = RunInfo[0]
        R = RunTools(RunNum)
        OutFiles = R.GetRunFiles(RunInfo['tStart'],'L')
        if not len(OutFiles):
            print "No output L2 files for run %s, no files to adjust"%RunNum
            exit(1)
        
        ProdVersion = "%s_%s"%(str(RunInfo['run_id']),str(RunInfo['production_version']))
        

        
        #startTime = dataclasses.I3Time(RunInfo['good_tstart'].year,ComputeTenthOfNanosec(RunInfo['good_tstart'],RunInfo['good_tstart_frac'])) 
        #stopTime = dataclasses.I3Time(RunInfo['good_tstop'].year,ComputeTenthOfNanosec(RunInfo['good_tstop'],RunInfo['good_tstop_frac'])) 
        GoodStart = dataclasses.I3Time(RunInfo['good_tstart'].year,ComputeTenthOfNanosec(RunInfo['good_tstart'],RunInfo['good_tstart_frac'])) 
        GoodEnd = dataclasses.I3Time(RunInfo['good_tstop'].year,ComputeTenthOfNanosec(RunInfo['good_tstop'],RunInfo['good_tstop_frac'])) 
       
       
        firstGood, lastGood, L2Files = GetGoodSubruns(OutFiles,GoodStart,GoodEnd,ProdVersion)
        print "first Good File: ",firstGood
        print "last Good file: ",lastGood
        
        RemoveBadSubRuns(L2Files,firstGood,lastGood,1)

        TrimFile(firstGood,GoodStart,GoodEnd)
        TrimFile(lastGood,GoodStart,GoodEnd)
        

    except Exception, err:
        raise Exception("Error: %s "%str(err)) 
