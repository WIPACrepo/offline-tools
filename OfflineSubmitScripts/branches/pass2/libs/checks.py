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

from libs.files import GetSubRunStartStop, GetGoodSubruns, remove_path_prefix
from libs import dbtools

ICECUBE_GCDDIR = lambda x : "/data/exp/IceCube/%s/filtered/level2pass2/VerifiedGCD" %str(x)
ICECUBE_DATADIR = lambda x : "/data/exp/IceCube/%s/filtered/level2pass2/" %str(x)

def runs_already_submitted(dbs4_, runs, logger, dryrun):
    """
    Checks if all runs have already been submitted. In fact, it checks if the `submitted` flag in
    `grl_snapshot_info_pass2` is set to `1`.
    
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

    Runs = dbs4_.fetchall("""SELECT run_id, submitted FROM i3filter.grl_snapshot_info_pass2
                                    WHERE run_id IN (%s) AND (good_i3=1 OR good_it=1)"""%(','.join([str(r) for r in runs])),UseDict=True)

    Abort = False
    for Run in Runs:
        if not Run['submitted']:
            logger.error("""Run %s has not been submitted yet."""%(Run['run_id']))
            Abort = True

    if not Abort:
        logger.info('Passed resubmission check. All runs are subject to be resubmitted.')

    return not Abort

def CheckFiles(r, logger, dataset_id, season, dryrun = False, no_pass2_gcd_file = False, missing_output_files = None, force = False, accelerate = False, pass2_lost_files = []):
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
        if not force:
            return 1
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
    
    R = RunTools(r['run_id'], passNumber = 2)
    InFiles = R.GetRunFiles(r['tStart'],'P', season = season)
    OutFiles = R.GetRunFiles(r['tStart'],'L', season = season)
   
    # Remove all BadRuns from InFiles:
    bad_runs = dbtools.get_bad_sub_runs(dbs4 = dbs4_, dataset_id = dataset_id, run_id = r['run_id'], logger = logger)

    for bad_run in bad_runs:
        path = os.path.join(bad_run['path'], bad_run['name'])[5:]
        InFiles = filter(lambda e: e != path, InFiles)

    ProdVersion = "%s_%s/"%(str(r['run_id']),str(r['production_version']))
    
    Files2Check = []
    
    # check for multiple GCD files in out Dir, usually results from re-processing
    
    GCDName = [f for f in OutFiles if "GCD" in f and ProdVersion in f]

    if len(GCDName)!=1:
        logger.warning("Either None or more than 1 GCD file in output dir for run=%s"%str(r['run_id']))
        if not force:
            return 1
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
    
    GCDName = GCDName[0]
    GCDName = os.path.join(ICECUBE_GCDDIR(r['tStart'].year),os.path.basename(GCDName))

    if no_pass2_gcd_file:
        GCDName = GCDName.replace('/level2pass2/', '/level2/')

    if not os.path.isfile(GCDName):
        logger.warning("No Verified GCD file for run=%s, production_version%s"%\
               (str(r['run_id']),str(r['production_version'])))
        if not force:
            return 1
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
    
    Files2Check.append(GCDName)    
    
    L2Files = [f for f in OutFiles if "GCD" not in f \
                   and "txt" not in f and "root" not in f\
                   and "EHE" not in f and "IT" not in f \
                   and "log" not in f
                   and ProdVersion in f]  
    L2Files.sort()
    
    logger.debug('input files = %s' % InFiles)
    logger.debug('l2 files = %s' % L2Files)

    if len(InFiles) != len(L2Files):
        logger.warning("No. of Input and Output files don't match for run=%s, production_version=%s" %(str(r['run_id']),str(r['production_version'])))

        # Find missing pieces:
        ifiles = {int(f.split('Subrun00000000_00')[1].split('.')[0]): f for f in InFiles}
        ofiles = {int(f.split('Subrun00000000_00')[1].split('.')[0]): f for f in L2Files}

        # Missing input files
        mif = list(set(ofiles.keys()) - set(ifiles.keys()))

        # Missing output files
        mof = list(set(ifiles.keys()) - set(ofiles.keys()))

        if len(mif):
            logger.error('Missing input files:')
            for i in mif:
                logger.error('  %s: output file %s' % (i, ofiles[i]))

        if len(mof):
            logger.error('Missing output files:')
            for i in mof:
                logger.error('  %s: input file (%s) %s' % (i, 'exists' if os.path.exists(ifiles[i]) else 'missing', ifiles[i]))

                if missing_output_files is not None:
                    missing_output_files.append({'id': i, 'input': ifiles[i], 'input_exists': os.path.exists(ifiles[i]), 'run_id': r['run_id']})

        if not force:
            return 1
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')

    for p in InFiles:
        l = os.path.join(os.path.dirname(L2Files[0]),
                        os.path.basename(p).replace("PFFilt_PhysicsFiltering","Level2pass2_IC86.%s_data" % season)
                                           .replace(".tar",".i3"))\
                .replace('PFDST_PhysicsTrig_PhysicsFiltering', 'Level2pass2_IC86.%s_data' % season)\
                .replace('PFDST_TestData_PhysicsFiltering', 'Level2pass2_IC86.%s_data' % season)\
                .replace('PFDST_PhysicsFiltering', 'Level2pass2_IC86.%s_data' % season)\
                .replace('.gz', '.zst')\
                .replace('.bz2', '.zst')
   
        logger.debug("looking for file %s" % l)
 
        if not os.path.isfile(l):
            logger.warning("At least one output file %s does not exist for input file %s"%(l,p))
            if not force:
                return 1
            else:
                logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')

        Files2Check.append(p)
        Files2Check.append(l)
    
    FilesInfo = {}
    for f in Files2Check:
        folder = os.path.dirname(f)
        name = os.path.basename(f)

        if name not in FilesInfo:
            FilesInfo[name] = []

        FilesInfo[name].append(folder)

    Files2CheckS = ','.join(["'%s'" % f for f in FilesInfo.keys()])

    FilesInDbTmp = dbs4_.fetchall("SELECT name, path FROM i3filter.urlpath WHERE dataset_id = %s AND name IN (%s)" % (dataset_id, Files2CheckS))

    FilesInDb = []
    for row in FilesInDbTmp:
        path = remove_path_prefix(row[1]).rstrip('/')

        if path in FilesInfo[row[0]]:
            FilesInDb.append(os.path.join(path, row[0]))

    FilesInDb = list(set(FilesInDb))

    if len(Files2Check) != len(FilesInDb):
        logger.warning("Some file records don't exist for run=%s, production_version=%s" %(str(r['run_id']),str(r['production_version'])))
        PrintVerboseDifference(Files2Check,FilesInDb,logger)
        if not force:
            return 1
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')

    # Check MD5 sums
    outputFileInfos = dbs4_.fetchall("""SELECT sub_run, name, path, size, md5sum
                                        FROM run r
                                        JOIN urlpath u 
                                            ON u.queue_id = r.queue_id 
                                            AND u.dataset_id = r.dataset_id 
                                        WHERE   r.dataset_id = %s 
                                            AND type = 'PERMANENT'
                                            AND transferstate <> 'DELETED' 
                                            AND run_id = %s""" % (dataset_id, r['run_id']),
                                    UseDict = True)

    for i, subrunInfo in enumerate(outputFileInfos):
        path = os.path.join(remove_path_prefix(subrunInfo['path']), subrunInfo['name'])

        if path.endswith('.zst') and not accelerate:
            try:
                sub.check_output(['/cvmfs/icecube.opensciencegrid.org/py2-v3/RHEL_6_x86_64/bin/zstd', '--test', path])
            except sub.CalledProcessError as e:
                logger.error('File {0} is corrupted: {1}'.format(path, str(e)))
                return 1

        logger.info('Validate checksum %s/%s' % (i + 1, len(outputFileInfos)))

        if path in OutFiles or os.path.isfile(path):
            if not accelerate:
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

