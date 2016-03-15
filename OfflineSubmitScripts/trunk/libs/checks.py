#!/usr/bin/env python

"""
Functions to perform checks.
"""

import glob
import os
import stat
import subprocess as sub

try:
    from icecube import icetray,dataclasses,dataio
except:
    warn("No env-shell loaded")

import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

m_live = live.MySQL()    
dbs4_ = dbs4.MySQL()   
dbs2_ = dbs2.MySQL()    

from RunTools import RunTools

from libs.files import GetSubRunStartStop,GetGoodSubruns

ICECUBE_GCDDIR = lambda x : "/data/exp/IceCube/%s/filtered/level2/VerifiedGCD" %str(x)
ICECUBE_DATADIR = lambda x : "/data/exp/IceCube/%s/filtered/level2/" %str(x)

def runs_already_submitted(dbs4_, StartRun, EndRun, logger, dryrun):
    """
    Checks if all runs have already been submitted. In fact, it checks if the `submitted` flag in
    `grl_snapshot_info` is set to `1`.
    
    Args:
        dbs4_ (SQLClient_dbs4): The SQL client for dbs4.
        StartRun (int): First run that has to be checked
        EndRun (int): Last run that has to be checked
        logger (logging.Logger): The logger
        dryrun (bool): Is it a dryrun?

    Returns:
        bool: Returns `True` if the runs passed this check.
    """
    
    logger.info('Check runs for resubmission.')

    Runs = dbs4_.fetchall("""SELECT run_id, submitted FROM i3filter.grl_snapshot_info
                                    WHERE run_id BETWEEN %s AND %s AND (good_i3=1 OR good_it=1)"""%(StartRun, EndRun),UseDict=True)

    Abort = False
    for Run in Runs:
        if not Run['submitted']:
            logger.error("""Run %s has not been submitted yet."""%(Run['run_id']))
            Abort = True

    if not Abort:
        logger.info('Passed resubmission check. All runs are subject to be resubmitted.')

    return not Abort

def CheckFiles(r,logger,dryrun=False):
    """
    Check if there are as many L2 files as there are PFFilt files. 
    Check for GCD files and database consistency;

    Args:
        r (dict): extracted from database

    Returns:
        bool: 0 if everything is fine, 1 if errors
    """

    if not r['GCDCheck'] or not r['BadDOMsCheck']:
        logger.info("GCDCheck or BadDOMsCheck failed for run=%s, production_version%s" %(str(r['run_id']),str(r['production_version'])))
        return 1
    
    R = RunTools(r['run_id'])
    InFiles = R.GetRunFiles(r['tStart'],'P')
    OutFiles = R.GetRunFiles(r['tStart'],'L')
    
    ProdVersion = "%s_%s/"%(str(r['run_id']),str(r['production_version']))
    
    Files2Check = []
    
    # check for multiple GCD files in out Dir, usually results from re-processing
    
    GCDName = [f for f in OutFiles if "GCD" in f and ProdVersion in f]

    if len(GCDName)!=1:
        logger.warning("Either None or more than 1 GCD file in output dir for run=%s"%str(r['run_id']))
        return 1
    
    GCDName = GCDName[0]
    GCDName = os.path.join(ICECUBE_GCDDIR(r['tStart'].year),os.path.basename(GCDName))

    if not os.path.isfile(GCDName):
        logger.warning("No Verified GCD file for run=%s, production_version%s"%\
               (str(r['run_id']),str(r['production_version'])))
        return 1
    
    Files2Check.append(GCDName)    
    
    L2Files = [f for f in OutFiles if "GCD" not in f \
                   and "txt" not in f and "root" not in f\
                   and "EHE" not in f and "IT" not in f \
                   and "log" not in f
                   and ProdVersion in f]  
    L2Files.sort()
    

    if len(InFiles) != len(L2Files):
        logger.warning("No. of Input and Output files don't match for run=%s, production_version=%s" %(str(r['run_id']),str(r['production_version'])))
        return 1

    for p in InFiles:
        l = os.path.join(os.path.dirname(L2Files[0]),os.path.basename(p).replace\
             ("PFFilt_PhysicsFiltering","Level2_IC86.2015_data").replace\
             (".tar",".i3").replace\
             ("Subrun00000000_","Subrun"))
    
        if not os.path.isfile(l):
            logger.warning("At least one output file %s does not exist for input file %s"%(l,p))
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
        logger.warning("Some file records don't exist for run=%s, production_version=%s" %(str(r['run_id']),str(r['production_version'])))
        PrintVerboseDifference(Files2Check,FilesInDb,logger) 
        return 1
    
    # make symlink to latest output dir
    baseDir = ICECUBE_DATADIR(r["tStart"].year) + "/%s%s"%\
              (str(r['tStart'].month).zfill(2),str(r['tStart'].day).zfill(2))
    OutDirs = [g.split("_")[-1] for g in glob.glob(os.path.join(baseDir,"Run00%s_*"%r['run_id']))]
    OutDirs.sort(key=int)
    LatestDir = os.path.join(baseDir,"Run00%s_%s"%(r['run_id'],OutDirs[-1]))
    
    LinkDir = os.path.join(baseDir,"Run00%s"%r['run_id'])
    
    if os.path.lexists(LinkDir):
        if not dryrun: sub.call(["rm","%s"%LinkDir])
    if not dryrun: ln_ret = sub.call(["ln","-s","%s"%LatestDir,"%s"%LinkDir])
    if dryrun: ln_ret = False
    if ln_ret:
        logger.warning("Could not make symlink to latest production for run=%s"%\
               (str(r['run_id'])))
        return 1
    
    # all checks passed
    return 0
    

def PrintVerboseDifference(FList1,FList2,logger):
    """
    Compare two lists and prints the differences

    Args:
        FList1 (list): ..
        FList2 (list): ..
        logger (logging.Logger): logger instance to use
    Returns:
        None
    """
    tmp_diff = list(set(FList1) - set(FList2))
    if len(tmp_diff):
        tmp_diff.sort()
        logger.warning("entries on disk but not in db")
        for t_d in tmp_diff:
            logger.info( t_d.__repr__())
    tmp_diff = list(set(FList2) - set(FList1))
    if len(tmp_diff):
        tmp_diff.sort()
        logger.warning("entries in db but not on disk")
        for t_d in tmp_diff:
            logger.info(t_d.__repr__())

###################################################

def pffilt_size_and_permission(runId, year, month, day, logger, verbose = True):
    """
    Checks if the PFFilt files of the given run has a proper file (size > 0) and proper file permission.

    Args:
        runId (int): The Run Id
        year (int): Year of the run
        month (int): Month of the run
        day (int): Day of the run
        logger (logging.Logger): The logger
        verbose (bool): Be more verbose? Default is `True`

    Returns:
        dict: Returns a dictionary with three lists of files: empty files, files with wrong permissions,
              and files that are empty and have wrong permissions:
              `{'empty': [], 'permission': [], 'emptyAndPermission': []}`
    """

    result = {'empty': [], 'permission': [], 'emptyAndPermission': []}

    if month < 10:
        month = '0' + str(month)

    if day < 10:
        day = '0' + str(day)

    path = '/data/exp/IceCube/' + str(year) + '/filtered/PFFilt/' + str(month) + str(day) + '/PFFilt_*' + str(runId) + '*.tar.bz2'

    if verbose:
        logger.info('Check run ' + str(runId) + ': ')

    files = glob.glob(path)
    files.sort()

    for file in files:
        st = os.stat(file)
        size = st.st_size

        empty = False
        perm = False

        if size == 0:
            empty = True

        if not (st.st_mode & stat.S_IRGRP):
            perm = True

        if empty and perm:
            result['emptyAndPermission'].append(file)
        elif empty:
            result['empty'].append(file)
        elif perm:
            result['permission'].append(file)

    if verbose:
        if len(result['empty']) > 0 or len(result['permission']) > 0 or len(result['emptyAndPermission']) > 0:
            logger.info("  %s empty files; %s files with wrong permissions; %s empty files with wrong permissions;"%(str(len(result['empty'])),
                                                                                                                  str(len(result['permission'])),
                                                                                                                  str(len(result['emptyAndPermission']))))
        else:
            logger.info('  everything is allright')

    return result

def has_sps_gcd_file(runId, year, month, day, logger):
    """
    Checks if the SPS GCD file exists for this run.

    Args:
        runId (int): The Run Id
        year (int): Year of the run
        month (int): Month of the run
        day (int): Day of the run
        logger (logging.Logger): The logger

    Returns:
        bool: `True` if the SPS GCD file exists for this run. Otherwise, `False` is returned.
    """

    if month < 10:
        month = '0' + str(month)

    if day < 10:
        day = '0' + str(day)

    path = '/data/exp/IceCube/' + str(year) + '/internal-system/sps-gcd/' + str(month) + str(day) + '/SPS-GCD_Run*' + str(runId) + '*.i3.tar.gz'

    files = glob.glob(path)

    return len(files) > 0

def leap_second_affected_subruns(run_id, good_tstart, good_tstop, production_version, season, logger):
    """
    Checks if the first and the last subrun is affected by the leap second bug.

    Args:
        run_id (int): The run id
        good_tstart (I3Time): The `good_tstart`
        good_tstop (I3Time): The `good_tstop`
        production_version (int): The production version of the run
        season (int): The season of the run (e.g. 2015)
        logger (logging.Logger): the logger.

    Returns:
        list: Returns a list. If the list is empty, no issues are found. If the list contains `start`, an issue
              with the first subrun is found. It it contains `end`, an issue with the last subrun is found.
    """
    run_tools = RunTools(run_id)
    out_files = run_tools.GetRunFiles(good_tstart.date_time, 'L')

    version = "%s_%s"%(str(run_id),str(production_version))

    first_good, last_good, L2_files = GetGoodSubruns(out_files, good_tstart, good_tstop, version)

    first_good_start_time, first_good_stop_time = GetSubRunStartStop(first_good, logger)
    last_good_start_time, last_good_stop_time = GetSubRunStartStop(last_good, logger)

    result = []

    if first_good_start_time < good_tstart:
        result.append('start')
        logger.error("Start time of first good subrun is smaller than time in database:")
        logger.error("    file start time: %s"%first_good_start_time)
        logger.error("    db start time  : %s"%good_tstart)
        logger.error("    file           : %s"%first_good)

    if last_good_stop_time.date_time.second + 1 == good_tstop.date_time.second:
        result.append('end')
        logger.error("End time of last good subrun around 1 sec smaller than end time in database:")
        logger.error("    file end time  : %s"%last_good_stop_time)
        logger.error("    db end time    : %s"%good_tstop)
        logger.error("    file           : %s"%last_good)

    return result

def leap_second_affected_gcd(run_id, time, season, logger):
    """
    Checks if the GCD file of this run contains leap second affected times.

    *WARNING: It only works correctly if you know that `GoodRunStartTime` is wrong! Is `GoodRunStartTime` correct,
    it will give you false positives!!!!111!!!1*

    In fact, it checks the `GoodRunStartTime` and the I3DetectorStatus `start_time`.
    It compares the seconds of both times with the second of the `time` argument.

    Args:
        run_id (int) the run id
        time (I3Time): The time from the database

    Returns:
        int: It returns `1` if a time mismatch is found. Otherwise, `0` is returned.
    """

    # Get the datetime object
    time = time.date_time

    path = "/data/exp/IceCube/%s/filtered/level2/AllGCD/Level2_IC86.%s_data_Run%s_*_GCD.i3.gz"%(time.year, season, str(run_id).zfill(8))

    logger.debug("Looking for files: %s"%path)

    files = glob.glob(path)

    for file in files:
        logger.info("Check file %s"%file)

        i3file = dataio.I3File(file)
        
        # Skip G and D frame
        i3file.pop_frame()
        i3file.pop_frame()

        # Get D frame
        frame = i3file.pop_frame()

        status = frame['I3DetectorStatus']
        goodstarttime = frame['GoodRunStartTime']

        if status.start_time.date_time.second == goodstarttime.date_time.second or status.start_time.date_time.second != time.second:
            logger.error("Time mismatch: I3DetectorStatus.start_time = %s"%status.start_time)
            logger.error("               GoodRunStartTime            = %s"%goodstarttime)
            logger.error("               good_tstart                 = %s"%time)
            return 1
        else:
            return 0

