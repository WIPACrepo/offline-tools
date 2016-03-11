"""
Tools to work with files
"""

import os
from warnings import warn

try:
    from I3Tray import I3Tray
except:
    warn("No env-shell loaded")

########################################################

def GetSubRunStartStop(FileName,logger,fromgapsfile=True):
    """
    Attempt to get the start and stop times from a subrun

    Args:
        FileName (str): An *.i3.* file
        logger (logging.Logger): the logger instance to use

    Keyword Args:
        getStop (bool): Return stop time
        fromgapsfile (bool): Attempt to use the gaps.txt file first
                             to get the start and stop time (cheaper)
    Returns:
        starttime (I3Time)
        if getStop is set, a tuple (starttime,endtime) is returned 
    
    """
    if fromgapsfile:
        # attempt to get subrun start and stop times from gaps file ... cheaper
        gaps_file = str(FileName).replace(".i3.bz2","_gaps.txt")
        try:
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
                return start_,stop__
        except:
            logger.warning("Attempt to get times from gaps file failed...")
            logger.warning("Trying to get times from file!")
            
    # try getting times from .i3 files if attempt from .gaps.txt file fails   
    start_ = (dataio.I3File(FileName)).pop_frame()['I3EventHeader'].start_time
    i3file = dataio.I3File(FileName)
    if not hasattr(i3file,"next"):
        for j in i3file:
            frame = j
    else:
        while True: frame = i3file.next()
    stop_ = frame['I3EventHeader'].start_time
    return start_,stop_

######################################################

def GetGoodSubruns(OutFiles,GoodStartTime,GoodStopTime,ProdVersion):
    """
    Find subruns of this run in the time range between "good" start and 
    stop time
    
    Args:
        OutFiles (list): L2 run files
        GoodStartTime (I3Time): good run start time
        GoodStopTime (I3Time): good stop time
        ProdVersion (int): current production version of this run

    """

    L2Files = [f for f in OutFiles if "GCD" not in f \
               and "txt" not in f and "root" not in f\
               and "EHE" not in f and "IT" not in f \
               and "log" not in f
               and ProdVersion in f]
    
    L2Files.sort()
    firstGood = L2Files[0]
    thrownawaysome = False
    for f in L2Files:
        start_,end_ = GetSubRunStartStop(f,getStop=1)
        # There might be gaps! Don't ask why...
        if end_ <= GoodStartTime: # This clearly should be thrown away
            thrownawaysome = True
            continue
        if end_ >= GoodStartTime:
            if start_<= GoodStartTime:
                firstGood = f
                break
            if start_ > GoodStartTime and thrownawaysome:
                firstGood = f # Mind the gap!
                break

    lastGood = L2Files[-1]
    thrownawaysome = False
    for f in reversed(L2Files):
        start_,end_ = GetSubRunStartStop(f,getStop=1)
        if start_ >= GoodStopTime: # This clearly should be thrown away
            thrownawaysome = True
            continue
        if start_ <= GoodStopTime:
            if end_>= GoodStopTime:
                lastGood = f
                break
            if end_ < GoodStopTime and thrownawaysome:
                lastGood = f # Mind the gap!
                break

    return firstGood,lastGood,L2Files
                
##############################################

def TrimFile(InFile,GoodStart,GoodEnd,logger):
    """
    Truncate a file and remove events which are not in the time interval
    [GoodStart,GoodEnd]

    Args:
        InFile (str): the name of the file to 
        GoodStart (): grl good start time
        GoodStop (): grl good stop time
        logger (logging.Logger): the logger instance to use
    """
    InFiles = [f for f in glob.glob(InFile.replace(".i3.bz2","*")) if ".i3" in f]
        
    for InFile_ in InFiles :
        logger.debug("Attempting to trim file %s" %InFile_)
    
        if (os.path.getsize(InFile_)) <= 14:
            logger.warning("skipping file=%s because it seems empty"%InFile_)
            continue
        TrimmedFile = "Trimmed_"+os.path.basename(InFile_)

        # the actual trimming is done with the TrimFileClass,
        # so it is required to boot I3Tray        
        tray = I3Tray()
        tray.AddModule("I3Reader","readL2File", filename = InFile_)
        tray.AddModule(TrimFileClass, 'Trim',
                       GoodStart = GoodStart,
                       GoodEnd = GoodEnd)
        tray.AddModule('I3Writer', 'FileWriter',
                        FileName = TrimmedFile,
                        Streams = [ I3Frame.DAQ,
                                    I3Frame.Physics]
                    )
        tray.AddModule("TrashCan","trash")
        tray.Execute()
        tray.Finish()
        
        logger.info("moving %s to %s"%(TrimmedFile,InFile_))
        os.system("mv -f %s %s"%(TrimmedFile,InFile_))
        
        dbs4_.execute("""update i3filter.urlpath u
                         set md5sum="%s", size="%s", transferstate="WAITING"
                         where u.dataset_id=%s 
                         and concat(substring(u.path,6),"/",u.name) = "%s" """%\
                        (str(FileTools(InFile_).md5sum()),str(os.path.getsize(InFile_)),"1883",InFile_))
        
        # re-write gaps.txt file using new (trimmed) .i3 file
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
                        
                logger.info("moving %s to %s"%(UpdatedGFile,GFile))
                os.system("mv -f %s %s"%(UpdatedGFile,GFile))
                dbs4_.execute("""update i3filter.urlpath u
                                set md5sum="%s", size="%s"
                                where u.dataset_id=%s 
                                and concat(substring(u.path,6),"/",u.name) = "%s" """%\
                                (str(FileTools(GFile).md5sum()),str(os.path.getsize(GFile)),"1883",GFile))

################################################################

def RemoveBadSubRuns(L2Files,firstGood,lastGood,logger,CleanDB = False):
    """
    Move sub runs which are not in the good run range 
    to a subfolder in the actual data folder of the run and
    optionally mark changes in the database

    Args:
        L2Files (list): the files of the run
        firstGood (str): name of the first file considered "good"
        lastGood (str): name of the last file considered "good"
        logger (logging.Logger): the logger instance to use

    KeywordArgs:
        CleanDB (bool): change the urlpath table and mark files as "DELETED"

    """ 

    logger.debug("Removing subruns not within good run range ....")
    L2Files.sort()
    firstGood_ = L2Files.index(firstGood)
    lastGood_ = L2Files.index(lastGood)
    Goodi3Files = L2Files[firstGood_:lastGood_+1]
    Badi3Files = [f for f in L2Files if f not in Goodi3Files]
    if not len(Badi3Files):
        logger.info("No complete subruns to be removed ...")
        return
    
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
        logger.debug("moving %s %s"%(r,BadName))
        os.system("mv %s %s"%(r,BadName))
    
    if CleanDB:
        # get <str> version of list to be used in DB cleanup
        ToBeRemovedS = """'""" + """','""".join(ToBeRemovedL) + """'"""

        dbs4_.execute("""update i3filter.urlpath u
                 join i3filter.job j on u.queue_id=j.queue_id
                 set u.transferstate="DELETED",j.status="BadRun"
                 where u.dataset_id=%s and j.dataset_id=%s
                 and concat(substring(u.path,6),"/",u.name) in (%s)"""%("1883","1883",ToBeRemovedS))




def get_rootdir():
    """
    Get the absolute path of the OfflineSubmitScripts_whatever
    FIXME: This is sensitive to the location of its implementation
    

    """
    thisdir = os.path.split(os.path.abspath(__file__))[0]
    rootdir = os.path.split(thisdir)[0] # go one up
    return rootdir

#############################################

def get_logdir(sublogpath=""):
    """
    Get the root log dir

    Keyword Args:
        sublogpath (str): path under ../log

    Returns:
        str
    """
    root_dir = get_rootdir()
    return os.path.join(root_dir,os.path.join("logs",sublogpath))

#############################################

def get_tmpdir():
    """
    Get the tmpdir under the root path

    Returns:
        str
    """

    root_dir = get_rootdir()
    return os.path.join(root_dir,"tmp")

#############################################

if __name__ == "__main__":
    for i in [get_rootdir(),get_logdir(),get_tmpdir()]:
        print i, os.path.exists(i)
