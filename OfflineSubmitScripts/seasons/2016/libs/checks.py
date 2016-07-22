#!/usr/bin/env python

"""
Functions to perform checks.
"""

import glob
import re
import os
import stat
import subprocess as sub
import sys

from warnings import warn

try:
    from icecube import icetray,dataclasses,dataio
except:
    warn("No env-shell loaded")

import config
sys.path.append(config.get_config().get('DEFAULT', 'ProductionToolsPath'))
sys.path.append(config.get_config().get('DEFAULT', 'SQLClientPath'))

import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

dbs4_ = dbs4.MySQL()   

from RunTools import RunTools
from FileTools import FileTools

from libs.files import GetSubRunStartStop, GetGoodSubruns

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

def CheckFiles(r, logger, dataset_id, season, dryrun = False):
    """
    Check if there are as many L2 files as there are PFFilt files. 
    Check for GCD files and database consistency;

    Args:
        r (dict): extracted from database
        dataset_id (int): The dataset id of the run

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
             ("PFFilt_PhysicsFiltering","Level2_IC86.%s_data" % season).replace\
             (".tar",".i3").replace\
             ("Subrun00000000_","Subrun"))
    
        if not os.path.isfile(l):
            logger.warning("At least one output file %s does not exist for input file %s"%(l,p))
            return 1

        Files2Check.append(p)
        Files2Check.append(l)
    
    Files2CheckS = """'""" + """','""".join(Files2Check) + """'"""
    
    FilesInDb = dbs4_.fetchall("""SELECT distinct name,concat(substring(u.path,6),"/",u.name)
                                  FROM i3filter.urlpath u
                                 WHERE u.dataset_id = %s AND
                                 concat(substring(u.path,6),"/",u.name) IN (%s) OR \
                                 concat(substring(u.path,6),u.name) IN (%s) """ %\
                                 (dataset_id, Files2CheckS, Files2CheckS))
    
    FilesInDb = [f[1].replace('//','/') for f in FilesInDb]

    if len(Files2Check) != len(FilesInDb):
        logger.warning("Some file records don't exist for run=%s, production_version=%s" %(str(r['run_id']),str(r['production_version'])))
        PrintVerboseDifference(Files2Check,FilesInDb,logger) 
        return 1
   
    # Check MD5 sums
    outputFileInfos = dbs4_.fetchall("""SELECT sub_run, name, path, size, md5sum
                                        FROM run r
                                        JOIN urlpath u 
                                            ON u.queue_id = r.queue_id 
                                            AND u.dataset_id = r.dataset_id 
                                        WHERE   r.dataset_id = %s 
                                            AND type = 'PERMANENT' 
                                            AND run_id = %s""" % (dataset_id, r['run_id']),
                                    UseDict = True)

    for subrunInfo in outputFileInfos:
        path = os.path.join(subrunInfo['path'][5:], subrunInfo['name'])

        if path in OutFiles:
            md5sum = FileTools(FileName = path, logger = logger).md5sum()

            # Check if checksum matches the checksum in DB
            if md5sum == subrunInfo['md5sum']:
                logger.debug("MD5 check sums match for %s" % path)
            else:
                logger.warning("MD5 check sum mismatch for %s" % path)
                return 1
        else:
            logger.warning("File %s is listed in the database as PERMANENT but doesn not exist" % path)
            return 1
 
    # make symlink to latest output dir
    baseDir = ICECUBE_DATADIR(r["tStart"].year) + "/%s%s"%\
              (str(r['tStart'].month).zfill(2),str(r['tStart'].day).zfill(2))
    OutDirs = [g.split("_")[-1] for g in os.listdir(baseDir) if re.search(r"^Run%s_[0-9]+$"%str(r['run_id']).zfill(8), g)]
    OutDirs.sort(key=int)
    LatestDir = "Run00%s_%s" % (r['run_id'], OutDirs[-1])
    
    LinkDir = os.path.join(baseDir, "Run00%s" % r['run_id'])
    
    if os.path.lexists(LinkDir):
        if not dryrun:
            sub.call(["rm", "%s" % LinkDir])

    logger.debug("Sym link command: ln -s %s %s" % (LatestDir, LinkDir))

    if not dryrun:
        ln_ret = sub.call(["ln", "-s", "%s" % LatestDir, "%s" % LinkDir])

    if dryrun:
        ln_ret = False

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

