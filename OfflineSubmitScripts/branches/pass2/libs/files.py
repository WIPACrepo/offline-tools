"""
Tools to work with files
"""

import os, time, glob, re
import subprocess as sub

import datetime

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

RUNINFODIR = lambda year : "/data/exp/IceCube/%s/filtered/level2pass2/RunInfo/" %str(year)
LEVEL2_DIR = lambda year : "/data/exp/IceCube/%s/filtered/level2pass2/" %str(year)

def MakeRunInfoFile(dbs4_, dataset_id, logger, dryrun = False):
    """
    Write the 'goodrun list': start stop and string information for every run

    Keyword Args:
        dbs4_ (SQLClient_dbs4.MySQL): The mysql client for dbs4
        dryrun (bool): Do not do anything serious if set

    Returns:
        None
    """
    from runs import get_run_lifetime

    RunInfo = dbs4_.fetchall("""SELECT * FROM i3filter.grl_snapshot_info_pass2 g
                                 JOIN i3filter.run_info_summary_pass2 r ON r.run_id=g.run_id
                                 JOIN i3filter.run jr ON jr.run_id=r.run_id
                                 WHERE jr.dataset_id=%s AND (g.good_i3 or g.good_it) AND g.submitted
                                 GROUP BY jr.run_id, g.snapshot_id
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

    if not dryrun and not os.path.exists(RUNINFODIR(ProductionYear)):
        os.mkdir(RUNINFODIR(ProductionYear))

    RI_File = open(RunInfoFile,'w')
    RI_FileV = open(RunInfoFileV,'w')
    
    RI_File.write("RunNum  Good_i3  Good_it  LiveTime(s) ActiveStrings   ActiveDoms     ActiveInIce        OutDir                                                  Comment(s)")
    RI_File.write("\n         (1=good 0=bad)  ")
    
    RI_FileV.write("RunNum  Good_i3  Good_it  LiveTime(s) ActiveStrings   ActiveDoms        ActiveInIce       OutDir                                                 Comments(s)")
    RI_FileV.write("\n         (1=good 0=bad)  ")
    
    for k in keys_:
        if not RunInfoDict[k]['validated']:
            #RI_File.write("\n%s  **Incomplete Processing or Not Validated**"%k)
            #RI_FileV.write("\n%s  **Incomplete Processing or Not Validated**"%k)
            continue
    
        StartTime = RunInfoDict[k]['tStart'] 

        LiveTime = round(get_run_lifetime(k, logger)['livetime'], 2)

        Comments = ""
        if config.is_test_run(int(k)):
            Comments = "IC86_%s 24hr test run" % ProductionYear
        
        ActiveStrings = "  "
        if RunInfoDict[k]['ActiveStrings'] is not None :  ActiveStrings = str(RunInfoDict[k]['ActiveStrings'])
        ActiveDOMs = "    "
        if RunInfoDict[k]['ActiveDOMs'] is not None :  ActiveDOMs = str(RunInfoDict[k]['ActiveDOMs'])
        ActiveInIceDOMs = "    "
        if RunInfoDict[k]['ActiveInIceDOMs'] is not None :  ActiveInIceDOMs = str(RunInfoDict[k]['ActiveInIceDOMs'])

        OutDir = os.path.join(LEVEL2_DIR(StartTime.year), "%s%s/Run00%s/"%\
                 (str(StartTime.month).zfill(2),\
                  str(StartTime.day).zfill(2),k))
    
        RI_File.write("\n%s     %s        %s        %s           %s          %s         %s          %s    %s"%\
                    (k,RunInfoDict[k]['good_i3'],RunInfoDict[k]['good_it'],\
                     LiveTime,ActiveStrings,ActiveDOMs,ActiveInIceDOMs, OutDir, Comments))
    
        RI_FileV.write("\n%s     %s        %s        %s           %s          %s            %s          %s   %s"%\
                    (k,RunInfoDict[k]['good_i3'],RunInfoDict[k]['good_it'],\
                     LiveTime,ActiveStrings,ActiveDOMs,ActiveInIceDOMs, os.path.realpath(OutDir), Comments)) 
        
    
    RI_File.close()
    RI_FileV.close()
   
    if not dryrun: 
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
    
    if not dryrun:   
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
        FirstEvent = None
        if InFile_ == InFile:
            TFile = dataio.I3File(InFile_)
            while TFile.more():
                frame = TFile.pop_frame()

                if 'I3EventHeader' in frame.keys():
                    if FirstEvent is None:
                        FirstEvent = frame['I3EventHeader']

                    LastEvent = frame['I3EventHeader']

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

def get_leap_second_file():
    return os.path.join(get_tmpdir(), 'leap-seconds.list')

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

def write_meta_xml_post_processing(dest_folder, level, script_file, logger, npx):
    import xml.etree.ElementTree as ET
    import xml.dom.minidom as minidom
    import libs.svn

    if npx:
        svn = libs.svn.SVN(get_rootdir(), logger, os.path.join(get_tmpdir(), 'svninfo.txt'))
    else:
        svn = libs.svn.SVN(get_rootdir(), logger)

    # Since it is the post processing, there should already be a meta file
    # If there is no meta file, display a warning.
    file_name = '' # assigned later

    if level not in ['L2', 'L3']:
        logger.critical("Level '%s' is not valid" % level)
        exit(1)
    else:
        if level == 'L2':
            file_name = config.get_config().get('L2', 'MetaFileName')
        elif level == 'L3':
            file_name = config.get_config().get('L3', 'MetaFileName')
    
    path = os.path.join(dest_folder, file_name)
    if not os.path.isfile(path):
        logger.warning("Meta file '%s' does not exist. That means that no meta information are avilable from main processing, and we can not proceed adding information." % path)
        logger.warning("Post processing of this run will continue but no meta information will be written.")
        return

    # Adding post processing information
    xml_tree = ET.parse(path)
    xml_root = xml_tree.getroot()
    xml_post_processing = ET.Element('Project')

    xml_name = ET.Element('Name')
    xml_version = ET.Element('Version')
    xml_date_time = ET.Element('DateTime')

    xml_name.text = os.path.join(svn.get('URL'), os.path.basename(script_file))
    xml_version.text = "Revision %s" % svn.get('Revision')
    xml_date_time.text = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    # Finding Plus section
    xml_plus = xml_root.find('Plus')
    if xml_plus is None:
        logger.critical("Cannot find 'Plus' elemnt in meta xml file %s" % path)
        exit(1)

    # The actual adding
    xml_post_processing.append(xml_name)
    xml_post_processing.append(xml_version)
    xml_post_processing.append(xml_date_time)

    xml_plus.append(xml_post_processing)

    # Writing file
    with open(path, 'w') as file:
        formatted_xml = minidom.parseString(ET.tostring(xml_root)).toprettyxml(indent = '    ')

        # It contains empty lines. Remove them
        formatted_xml = os.linesep.join([s for s in formatted_xml.splitlines() if s.strip()])

        logger.debug("Write meta file: %s" % path)
        file.write(formatted_xml)

def write_meta_xml_main_processing(dest_folder, dataset_id, run_id, level, run_start_time, run_end_time, logger):
    """
    Writes a meta XML file for a specific run.

    Utilizes a lot of information from the config/offline_processing.cfg, which also specifies
    which template file should be used.

    Args:
        dest_folder (str): Folder in which the file should be written
        dataset_id (int): The dataset id if the run
        run_id (int): The run id
        level (str): Can be 'L2' or 'L3'
        run_start_time (datetime.datetime): Start time of run
        run_end_time (datetime.datetime): End time of run
        logger (logger.Logger): The logger
    """

    conf = config.get_config()

    # Get all information that is required
    now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    season = config.get_season_by_run(run_id)
    ts_first_name = conf.get('PERSONNEL', 'FirstName')
    ts_last_name = conf.get('PERSONNEL', 'LastName')
    ts_email = conf.get('PERSONNEL', 'eMail')
    dif_creation_date = run_start_time.strftime('%Y-%m-%d')
    start_date_time = run_start_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_time = run_end_time.strftime('%Y-%m-%dT%H:%M:%S')
    subcategory = '' # assigned later
    subcategory_capitalized = '' # assigned later
    icerec_version = '' # assigned later
    file_name = '' # assigned later
    template_file = conf.get('DEFAULT', 'MetaFileTemplateMainProcessing')
    working_group = None # Will be assigned automatically if it is a L3 dataset

    template = ''
    with open(template_file, 'r') as file:
        template = file.read()

    if season == -1:
        logger.critical("Could not determine season for run %s. Check the run number and the config file." % run_id)
        exit(1)

    if level not in ['L2', 'L3']:
        logger.critical("Level '%s' is not valid" % level)
        exit(1)
    else:
        if level == 'L2':
            subcategory = 'level2'
            file_name = conf.get('L2', 'MetaFileName')

            icerec_version = os.path.basename(conf.get('L2', 'I3_SRC'))
        elif level == 'L3':
            subcategory = 'level3'
            
            l3_datasets = config.get_var_dict('L3', 'I3_SRC_', keytype = int)
            if dataset_id not in l3_datasets.keys():
                logger.critical("Dataset %s is not configured in config file." % dataset_id)
                exit(1)

            icerec_version = os.path.basename(l3_datasets[dataset_id])
            file_name = conf.get('L3', 'MetaFileName')

    subcategory_capitalized = subcategory.title()

    if level == 'L3':
        working_groups = config.get_var_dict('L3', 'WG', keytype = int)

        if working_groups[dataset_id] is None:
            logger.critical("Working group name is not defined for dataset %s. Check config file." % dataset_id)
            exit(1)

        subcategory_capitalized = "%s (%s)" % (subcategory_capitalized, working_groups[dataset_id])

    if not os.path.isdir(dest_folder):
        logger.critical("Folder '%s' does not exist" % dest_folder)
        exit(1)

    # Fill the template
    meta_file_content = template % (season, subcategory_capitalized, run_id,
                                    season, subcategory_capitalized, run_id,
                                    ts_first_name,
                                    ts_last_name,
                                    ts_email,
                                    dif_creation_date,
                                    start_date_time,
                                    end_date_time,
                                    subcategory,
                                    run_id,
                                    icerec_version,
                                    now)

    path = os.path.join(dest_folder, file_name)
    with open(path, 'w') as file:
        logger.debug("Write meta file: %s" % path)
        file.write(meta_file_content)

#############################################

def tar_log_files(run_path, dryrun, logger):
    import tarfile
    import sys

    files = glob.glob(os.path.join(run_path, '*.log*'))
    dest = os.path.join(run_path, 'logfiles.tar.bz2')

    logger.debug("Found %s log files in %s" % (len(files), run_path))

    if not len(files):
        logger.debug('No logfiles found in %s' % run_path)
        return

    logger.debug("Tar-File: %s" % dest)

    try:
        with tarfile.open(dest, 'w:bz2') as tar:
            for file in files:
                logger.debug("Adding %s to tar file" % file)
                tar.add(file)

        # Ok, let's delete the log files
        for file in files:
            logger.debug("Delete %s" % file)

            if not dryrun:
                os.remove(file)
    except:
       logger.error("Writing tar file/deleting log files: %s" % sys.exc_info()[0]) 

class GapsFile:
    def __init__(self, path, logger):
        self.__path = path
        self.__logger = logger
        self.__values = None

    def get_path(self):
        return self.__path

    def __get_sub_run_id_from_path(self):
        c = re.compile(r'^/.*Subrun0+\_([0-9]+).*txt$')
        return int(c.search(self.__path).groups()[0])

    def read(self, force = False):
        if self.__values is not None and not force:
            return

        self.__values = {}
    
        with open(self.__path, 'r') as file:
            for line in file:
                pair = line.split(':')
    
                if len(pair) != 2:
                    raise
    
                key = pair[0].strip()
                value = pair[1].strip()
    
                if pair[0] == 'First Event of File':
                    key = 'first event'
                elif pair[0] == 'Last Event of File':
                    key = 'last event'
    
                if pair[0] == 'First Event of File' or pair[0] == 'Last Event of File':
                    tmp = value.split(' ')
                    value = {'event': int(tmp[0].strip()),
                            'year': int(tmp[1].strip()),
                            'frac': int(tmp[2].strip())}
    
                if pair[0] == 'Gap Detected':
                    tmp = value.split(' ')
                    key = 'gap'
                    value = {'dt': float(tmp[0].strip()),
                            'prev_event_id': int(tmp[1].strip()),
                            'prev_event_frac': int(tmp[2].strip()),
                            'curr_event_id': int(tmp[3].strip()),
                            'curr_event_frac': int(tmp[4].strip())}

                    # A file can have several gaps
                    if key not in self.__values:
                        self.__values[key] = []
    
                if key == 'gap':
                    self.__values[key].append(value)
                else:
                    self.__values[key] = value
    
        self.__values['subrun'] = self.__get_sub_run_id_from_path()

    def has_gaps(self):
        return 'gap' in self.__values.keys()

    def get_gaps(self):
        if self.has_gap():
            return self.__values['gap']
        else:
            return None

    def get_sub_run_id(self):
        return self.__values['subrun']

    def get_run_id(self):
        return self.__values['Run']

    def get_first_event(self):
        return self.__values['first event']

    def get_last_event(self):
        return self.__values['last event']

    def get_file_livetime(self):
        return self.__values['File Livetime']

def insert_gap_file_info_and_delete_files(run_path, dryrun, logger, do_not_delete_files = False):
    import databaseconnection

    gaps_files = glob.glob(os.path.join(run_path, '*_gaps.txt'))

    if not len(gaps_files):
        logger.warning("No gaps files were found for %s" % run_path)
        return
    else:
        logger.info("%s gaps files were found that will be copied to the DB and then deleted" % len(gaps_files))

    sql = """INSERT INTO sub_runs_pass2 
                (run_id, sub_run, first_event, last_event, first_event_year, first_event_frac, last_event_year, last_event_frac, livetime)
             VALUES %s
             ON DUPLICATE KEY UPDATE first_event = VALUES(first_event),
                                     last_event = VALUES(last_event),
                                     first_event_year = VALUES(first_event_year),
                                     first_event_frac = VALUES(first_event_frac),
                                     last_event_year = VALUES(last_event_year),
                                     last_event_frac = VALUES(last_event_frac),
                                     livetime = VALUES(livetime)"""

    sub_runs = []
    gaps = []
    for file in gaps_files:
        logger.debug("File %s" % file)

       # if os.path.getsize(file) == 0:
       #     continue

        gf = GapsFile(file, logger)
        gf.read()
        sub_runs.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s)" % (gf.get_run_id(), \
                                                                  gf.get_sub_run_id(), \
                                                                  gf.get_first_event()['event'], \
                                                                  gf.get_last_event()['event'], \
                                                                  gf.get_first_event()['year'],\
                                                                  gf.get_first_event()['frac'],\
                                                                  gf.get_last_event()['year'],\
                                                                  gf.get_last_event()['frac'],\
                                                                  gf.get_file_livetime()))

        logger.debug("INSERT set: %s" % sub_runs[-1])

        if gf.has_gaps():
            for gap in gf.get_gaps():
                gap_insert_sql = 'INSERT INTO gaps_pass2 (run_id, sub_run, prev_event_id, curr_event_id, delta_time, prev_event_frac, curr_event_frac) VALUES (%s, %s, %s, %s, %s, %s, %s)'
                gaps.append(gap_insert_sql % (gf.get_run_id(), gf.get_sub_run_id(), gap['prev_event_id'],  gap['curr_event_id'],  gap['dt'],  gap['prev_event_frac'], gap['curr_event_frac']))

    if not dryrun:
        db = databaseconnection.DatabaseConnection.get_connection('filter-db', logger)

        if gaps:
            # Insert gaps
            logger.debug('Insert gaps into db')
            db.execute(';'.join(gaps))
   
        # Insert sub runs
        logger.debug('Insert sub runs into db')
        db.execute(sql % ','.join(sub_runs))

        if not do_not_delete_files:
            # Delete *_gaps.txt files
            for file in gaps_files:
                logger.debug("Deleting %s" % file)
                os.remove(file)
    
#############################################

def remove_path_prefix(path):
    """ 
    Removes `file:` or `gsiftp://gridftp.icecube.wisc.edu` from path.
    """

    prefix = ['file:', 'gsiftp://gridftp.icecube.wisc.edu']

    for p in prefix:
        if path.startswith(p):
            return path[len(p):]

    return path

#############################################
 
if __name__ == "__main__":
    for i in [get_rootdir(),get_logdir(),get_tmpdir()]:
        print i, os.path.exists(i)

