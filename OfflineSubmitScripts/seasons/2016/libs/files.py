"""
Tools to work with files
"""

import os,time,glob
import subprocess as sub

from warnings import warn
from logger import DummyLogger

import config

import cPickle

try:
    from i3tools import TrimFileClass
    from I3Tray import I3Tray
    from icecube import icetray,dataclasses,dataio
except:
    warn("No env-shell loaded")

RUNINFODIR = lambda year : "/data/exp/IceCube/%s/filtered/level2/RunInfo/" %str(year)
LEVEL2_DIR = lambda year : "/data/exp/IceCube/%s/filtered/level2/" %str(year)

def MakeRunInfoFile(dbs4_, dataset_id, logger, dryrun = False):
    """
    Write the 'goodrun list': start stop and string information for every run

    Keyword Args:
        dbs4_ (SQLClient_dbs4.MySQL): The mysql client for dbs4
        dryrun (bool): Do not do anything serious if set

    Returns:
        None
    """
    RunInfo = dbs4_.fetchall("""SELECT * FROM i3filter.grl_snapshot_info g
                                 JOIN i3filter.run_info_summary r ON r.run_id=g.run_id
                                 JOIN i3filter.run jr ON jr.run_id=r.run_id
                                 WHERE jr.dataset_id=%s AND (g.good_i3 or g.good_it) AND g.submitted
                                 GROUP BY jr.run_id
                                 ORDER BY g.run_id,g.production_version""" % dataset_id, UseDict=True)
    
    RunInfoDict = {}
    for r in RunInfo:
        RunInfoDict[r['run_id']] = r
    keys_ = RunInfoDict.keys()
    keys_.sort()

    if len(keys_) == 0:
        return
    
    ProductionYear = str(RunInfoDict[keys_[0]]['tStart'].year)
    
    LatestProductionVersion = str(RunInfoDict[keys_[-1]]['production_version'])
    
    RunInfoFile = RUNINFODIR(ProductionYear) + "IC86_%s_GoodRunInfo_%s_"%(ProductionYear,LatestProductionVersion)+time.strftime("%Y_%m_%d-%H_%M_%S",time.localtime()) + ".txt"
    
    RunInfoFileV = RUNINFODIR(ProductionYear) + "IC86_%s_GoodRunInfo_%s_Versioned_"%(ProductionYear,LatestProductionVersion)+time.strftime("%Y_%m_%d-%H_%M_%S",time.localtime()) + ".txt"
    if dryrun: RunInfoFile  = os.path.join(get_tmpdir(), "runinfo")
    if dryrun: RunInfoFileV = os.path.join(get_tmpdir(), "runinfoV")
    RI_File = open(RunInfoFile,'w')
    RI_FileV = open(RunInfoFileV,'w')
    
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
        if config.is_test_run(int(k)):
            Comments = "IC86_%s 24hr test run" % ProductionYear
        
        ActiveStrings = "  "
        if RunInfoDict[k]['ActiveStrings'] is not None :  ActiveStrings = str(RunInfoDict[k]['ActiveStrings'])
        ActiveDOMs = "    "
        if RunInfoDict[k]['ActiveDOMs'] is not None :  ActiveDOMs = str(RunInfoDict[k]['ActiveDOMs'])
        ActiveInIceDOMs = "    "
        if RunInfoDict[k]['ActiveInIceDOMs'] is not None :  ActiveInIceDOMs = str(RunInfoDict[k]['ActiveInIceDOMs'])

        OutDir = LEVEL2_DIR(StartTime.year) + "/%s%s/Run00%s/"%\
                 (str(StartTime.month).zfill(2),\
                  str(StartTime.day).zfill(2),k)
    
        RI_File.write("\n%s     %s        %s        %s           %s          %s         %s          %s    %s"%\
                    (k,RunInfoDict[k]['good_i3'],RunInfoDict[k]['good_it'],\
                     LiveTime,ActiveStrings,ActiveDOMs,ActiveInIceDOMs, OutDir, Comments))
    
        RI_FileV.write("\n%s     %s        %s        %s           %s          %s            %s          %s   %s"%\
                    (k,RunInfoDict[k]['good_i3'],RunInfoDict[k]['good_it'],\
                     LiveTime,ActiveStrings,ActiveDOMs,ActiveInIceDOMs, os.path.realpath(OutDir), Comments)) 
        
    
    RI_File.close()
    RI_FileV.close()
    
    LatestGoodRunInfo = glob.glob(RUNINFODIR(ProductionYear) + "IC86_%s_GoodRunInfo_%s_2*" % (ProductionYear, LatestProductionVersion))
    LatestGoodRunInfo.sort(key=lambda x: os.path.getmtime(x))
    LatestGoodRunInfo = LatestGoodRunInfo[-1]
    LatestGoodRunInfoRelativePath = LatestGoodRunInfo.replace(LEVEL2_DIR(ProductionYear), '')

    logger.debug("Sym link command: ln -s %s %s" % (LatestGoodRunInfoRelativePath, LEVEL2_DIR(ProductionYear) + "IC86_%s_GoodRunInfo.txt" % (ProductionYear)))

    if os.path.lexists(LEVEL2_DIR(ProductionYear) + "IC86_%s_GoodRunInfo.txt" % (ProductionYear)):
        if not dryrun:
            sub.call(["rm", LEVEL2_DIR(ProductionYear) + "IC86_%s_GoodRunInfo.txt" % (ProductionYear)])

    if not dryrun:
        sub.call(["ln", "-s", "%s" % LatestGoodRunInfoRelativePath, LEVEL2_DIR(ProductionYear) + "IC86_%s_GoodRunInfo.txt" % (ProductionYear)])
       
    LatestGoodRunInfoV = glob.glob(RUNINFODIR(ProductionYear) + "IC86_%s_GoodRunInfo_%s_Versioned*" % (ProductionYear, LatestProductionVersion))
    LatestGoodRunInfoV.sort(key=lambda x: os.path.getmtime(x))
    LatestGoodRunInfoV = LatestGoodRunInfoV[-1]
    LatestGoodRunInfoVRelativePath = LatestGoodRunInfoV.replace(LEVEL2_DIR(ProductionYear), '')

    logger.debug("Sym link command: ln -s %s %s" % (LatestGoodRunInfoVRelativePath, LEVEL2_DIR(ProductionYear) + "IC86_%s_GoodRunInfo_Versioned.txt" % (ProductionYear)))

    if os.path.lexists(LEVEL2_DIR(ProductionYear) + "IC86_%s_GoodRunInfo_Versioned.txt" % (ProductionYear)):
        if not dryrun:
            sub.call(["rm", LEVEL2_DIR(ProductionYear) + "IC86_%s_GoodRunInfo_Versioned.txt" % (ProductionYear)])

    if not dryrun:
        sub.call(["ln", "-s", "%s" % LatestGoodRunInfoVRelativePath, LEVEL2_DIR(ProductionYear) + "IC86_%s_GoodRunInfo_Versioned.txt" % (ProductionYear)])

def MakeTarGapsTxtFile(dbs4_, StartTime, RunId, datasetid, dryrun = False, logger = DummyLogger()):
    """
    Tar the gaps files together and update the urlpath table with 
    the newly created file

    Args:
        dbs4_ (SQLClient_dbs4.MySQL): The mysql client for dbs4
        StartTime (datetimd.datetime): Good start time
        RunId (int): run number
        datasetid (int): dataset number
    
    Keyword Args:
        dryrun (bool): Don't do anything if set
    
    Returns:
        None

    """

    from FileTools import FileTools

    OutDir = LEVEL2_DIR(str(StartTime.year)) + "%s%s/Run00%s/"%(str(StartTime.month).zfill(2), str(StartTime.day).zfill(2),RunId)
    
    OutTar = os.path.join(OutDir,"Run00"+str(RunId)+"_GapsTxt.tar")
    
    gapsFiles = glob.glob(os.path.join(OutDir,"*_gaps.txt"))
    gapsFiles = [os.path.basename(g) for g in gapsFiles]
    gapsFiles.sort()
    if not dryrun: sub.check_call(["tar","cf",OutTar,"-C",OutDir,gapsFiles[0]])
    for g in gapsFiles[1:]:
        if not dryrun: sub.check_call(["tar","rf",OutTar,"-C",OutDir,g])
    
    maxQId = dbs4_.fetchall("""SELECT max(u.queue_id) FROM i3filter.urlpath u join i3filter.run r on u.queue_id=r.queue_id
                 where r.dataset_id=%s and u.dataset_id=%s and r.run_id=%s""" % (datasetid, datasetid, RunId))
    
    if not dryrun:
        dbs4_.execute(""" update i3filter.urlpath u join i3filter.run r on u.queue_id=r.queue_id set u.transferstate="IGNORED"
              where r.dataset_id=%s and u.dataset_id=%s and r.run_id=%s and u.name like "%%_gaps.txt" """ % (datasetid, datasetid, RunId))
    
        dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","PERMANENT","%s","%s")\
                   on duplicate key update dataset_id="%s",queue_id="%s",name="%s",path="%s",type="PERMANENT",md5sum="%s",size="%s",transferstate="WAITING"  """% \
                             (str(datasetid),str(maxQId[0][0]),os.path.basename(OutTar),"file:"+os.path.dirname(OutTar)+"/",str(FileTools(OutTar, logger).md5sum()),str(os.path.getsize(OutTar)),\
                              str(datasetid),str(maxQId[0][0]),os.path.basename(OutTar),"file:"+os.path.dirname(OutTar)+"/",str(FileTools(OutTar, logger).md5sum()),str(os.path.getsize(OutTar))))
    return 

########################################################

def GetSubRunStartStop(FileName,logger=DummyLogger(),fromgapsfile=True):
    """
    Attempt to get the start and stop times from a subrun

    Args:
        FileName (str): An *.i3.* file

    Keyword Args:
        fromgapsfile (bool): Attempt to use the gaps.txt file first
                             to get the start and stop time (cheaper)
        logger (logging.Logger): the logger instance to use
    Returns:
        tuple -> (I3Time,I3Time)
    
    """
    if fromgapsfile:
        # attempt to get subrun start and stop times from gaps file ... cheaper
        gaps_file = str(FileName).replace(".i3.bz2","_gaps.txt")

        logger.debug("gaps file: %s" % gaps_file)

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
                return start_,stop_
        except Exception as e:
            logger.error("Attempt to get times from gaps file failed. Exception %s has arisen" %e.__repr__())
            logger.info("Trying to get times from file!")
            
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

def GetGoodSubruns(OutFiles, GoodStartTime, GoodStopTime, ProdVersion, logger = DummyLogger()):
    """
    Find subruns of this run in the time range between "good" start and 
    stop time
    
    Args:
        OutFiles (list): L2 run files
        GoodStartTime (I3Time): good run start time
        GoodStopTime (I3Time): good stop time
        ProdVersion (int): current production version of this run. WARNING: You need to pass "<RUN ID>_<PPRODUCTION VERSION>"!

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
        start_,end_ = GetSubRunStartStop(f, logger = logger)
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
        start_,end_ = GetSubRunStartStop(f, logger = logger)
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

def TrimFile(dbs4_, InFile, GoodStart, GoodEnd, dataset_id, logger = DummyLogger(), dryrun = False):
    """
    Truncate a file and remove events which are not in the time interval
    [GoodStart,GoodEnd]

    Args:
        dbs4_ (SQLClient_dbs4.MySQL): The mysql client for dbs4
        InFile (str): the name of the file to 
        GoodStart (): grl good start time
        GoodStop (): grl good stop time
        dataset_id (int): The dataset id

    Keyword Args:
        logger (logging.Logger): the logger instance to use
        dryrun (boo): if set, don't do anything
    """

    from FileTools import FileTools

    InFiles = [f for f in glob.glob(InFile.replace(".i3.bz2","*")) if ".i3" in f]
        
    for InFile_ in InFiles :
        logger.debug("Attempting to trim file %s" %InFile_)
   
        # bzip2 -f of a zero-sized file results in a 
        # 14 byte large file 
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
                       GoodEnd = GoodEnd,
                       logger = logger,
                       dryrun = dryrun)
        tray.AddModule('I3Writer', 'FileWriter',
                        FileName = TrimmedFile,
                        Streams = [ icetray.I3Frame.DAQ,
                                    icetray.I3Frame.Physics]
                    )
        tray.AddModule("TrashCan","trash")
        tray.Execute()
        tray.Finish()  
        if not dryrun:
            logger.info("moving %s to %s"%(TrimmedFile,InFile_))
            os.system("mv -f %s %s"%(TrimmedFile,InFile_))
        
            dbs4_.execute("""update i3filter.urlpath u
                         set md5sum="%s", size="%s", transferstate="WAITING"
                         where u.dataset_id=%s 
                         and concat(substring(u.path,6),"/",u.name) = "%s" """%\
                        (str(FileTools(InFile_, logger).md5sum()), str(os.path.getsize(InFile_)), dataset_id, InFile_))
        
        # re-write gaps.txt file using new (trimmed) .i3 file
        if InFile_ == InFile:
            TFile = dataio.I3File(InFile_)
            FirstEvent = TFile.pop_frame()['I3EventHeader']
            while TFile.more(): LastEvent = TFile.pop_frame()['I3EventHeader']
            TFile.close()
            GFile = InFile.replace(".i3.bz2","_gaps.txt")
            if os.path.isfile(GFile):
                UpdatedGFile = os.path.join(os.path.dirname(GFile),'Updated_'+os.path.basename(GFile))
                # for dryrun, write the stuff to a temporary file
                if dryrun:
                    UpdatedGFile = os.path.join(get_tmpdir(),os.path.split(UpdatedGFile)[1])
                    logger.info("--dryrun set, writing to temporary file %s" %UpdatedGFile)
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
                if not dryrun:
                    os.system("mv -f %s %s"%(UpdatedGFile,GFile))
                    dbs4_.execute("""update i3filter.urlpath u
                                set md5sum="%s", size="%s"
                                where u.dataset_id=%s 
                                and concat(substring(u.path,6),"/",u.name) = "%s" """%\
                                (str(FileTools(GFile, logger).md5sum()),str(os.path.getsize(GFile)), dataset_id, GFile))
                else: # here we actually have to do something
                      # as we haven't overwritten the original file
                      # we have a stale Trimmed_ file...
                    os.remove(TrimmedFile)

################################################################

def RemoveBadSubRuns(dbs4_, L2Files, firstGood, lastGood, dataset_id, logger=DummyLogger(), CleanDB = False, dryrun = False):
    """
    Move sub runs which are not in the good run range 
    to a subfolder in the actual data folder of the run and
    optionally mark changes in the database

    Args:
        dbs4_ (SQLClient_dbs4.MySQL): The mysql client for dbs4
        L2Files (list): the files of the run
        firstGood (str): name of the first file considered "good"
        lastGood (str): name of the last file considered "good"
        dataset_id (int): the dataset id

    KeywordArgs:
        CleanDB (bool): change the urlpath table and mark files as "DELETED"
        logger (logging.Logger): the logger instance to use
        dryrun (bool): Don't do anything...
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
        if not dryrun: os.mkdir(os.path.join(os.path.dirname(ToBeRemovedL[0]),"Bad_NotWithinGoodRunRange"))
    
    BadDirOutput = os.path.join(os.path.dirname(ToBeRemovedL[0]),"Bad_NotWithinGoodRunRange")
     
    for r in ToBeRemovedL:
        BadName = os.path.join(os.path.dirname(r),BadDirOutput,os.path.basename(r).replace("Level2","BadDontUse_Level2"))
        logger.debug("moving %s %s"%(r,BadName))
        if not dryrun: os.system("mv %s %s"%(r,BadName))
    
    if CleanDB:
        # get <str> version of list to be used in DB cleanup
        ToBeRemovedS = """'""" + """','""".join(ToBeRemovedL) + """'"""

        if not dryrun: dbs4_.execute("""update i3filter.urlpath u
                 join i3filter.job j on u.queue_id=j.queue_id
                 set u.transferstate="DELETED",j.status="BadRun"
                 where u.dataset_id=%s and j.dataset_id=%s
                 and concat(substring(u.path,6),"/",u.name) in (%s)"""%(dataset_id, dataset_id, ToBeRemovedS))

##############################################################

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

def get_existing_check_sums(logger, ChkSumFile = config.get_config().get('CacheCheckSums', 'CacheFile')):    
    """
    Get dictionary of precalculated check sums for PFFilt files. Caching makes submission faster.

    Args:
        logger (logging.Logger): The logger
        ChkSumFile (str): Path to file with check sums

    Returns:
        dict: Dictionary of files and MD5 check sums. If the file is not found or cannot be opened
              an empty directory is returned, an error is logged, and an exception is logged.
    """

    logger.debug("Path to cached check sums: %s"%ChkSumFile)

    ExistingChkSums = {}
    try:
        ExistingChkSums = cPickle.load(open(ChkSumFile,"rb"))
        return ExistingChkSums
    except Exception,err:
        logger.error('Cannot retrieve cached check sums')
        logger.error('Return empty dict: {}')
        logger.exception(Exception)
        return ExistingChkSums

#############################################

if __name__ == "__main__":
    for i in [get_rootdir(),get_logdir(),get_tmpdir()]:
        print i, os.path.exists(i)
