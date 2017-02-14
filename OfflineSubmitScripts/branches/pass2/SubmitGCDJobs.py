#!/usr/bin/env python

"""
Submits the jobs to generate the GCD files.
It generates the folders for log files and the conder submit file. Finally, it submits the file ;)
"""

#############################################################################
#
#       General Description:    set run status in i3filter.pre_processing_checks table of dbs4
#                               gather information from i3live, dbs2 and input PFFilt files
#
#       General Usage: python SubmitGCDJobs_2015.py [ -s startRun -e EndRun]
#
# Copyright: (C) 2014 The IceCube collaboration
#
# @file    $Id$
# @version $Revision$

# @date    05/04/2015
# @author  Oladipo Fadiran <ofadiran@icecube.wisc.edu>
#
#############################################################################

import sys, os
import glob
import datetime
from dateutil.relativedelta import *
import time
import cPickle
import subprocess
from libs.argparser import get_defaultparser
from libs.logger import get_logger
from libs.files import get_logdir, get_rootdir, get_tmpdir
import libs.config

##-----------------------------------------------------------------
## setup DB
##-----------------------------------------------------------------

import SQLClient_i3live as live
m_live = live.MySQL()

import SQLClient_dbs4 as dbs4
dbs4_ = dbs4.MySQL()


if __name__ == '__main__':
    config = libs.config.get_config()

    CONDOR_SUBMIT_FILE = config.get('GCDGeneration', 'TmpCondorSubmitFile')
    I3BUILD_DIR = config.get('L2', 'I3_BUILD')

    #------------------------------------------------------------------

    parser = get_defaultparser(__doc__, dryrun = True)
    ##-----------------------------------------------------------------
    parser.add_argument("-s", "--startrun", type=int, required = True, default=-1,
                      dest="STARTRUN", help="Start generating GCD files from this run")
    
    parser.add_argument("-e", "--endrun", type=int, required = True, default=-1,
                      dest="ENDRUN", help="End generating GCD files at this run")
    
    parser.add_argument("-r", "--reproduce", action="store_true", default=False,
                  dest="REPRODUCE", help="Regenerate GCD file even if already attempted")
    
    parser.add_argument("-b", "--i3build", default=I3BUILD_DIR,
                      dest="I3BUILD", help="Icerec build directory")
    
    
    parser.add_argument("-p", "--pythonscriptdir", default=get_rootdir(),
                      dest="PythonScriptDir", help="directory containing python scripts to be used for GCD generation and auditing")
   
    options = parser.parse_args() 

    # Logger
    LOGFILE=os.path.join(get_logdir(sublogpath = 'MainProcessing'), 'SubmitGCDJobs_')
    logger = get_logger(options.loglevel, LOGFILE)
    logger.debug("CONDOR_SUBMIT_FILE = %s"%CONDOR_SUBMIT_FILE)
    ##-----------------------------------------------------------------
    ## Check and store arguments
    ##-----------------------------------------------------------------
    
    dryrun = options.dryrun

    START_RUN = options.STARTRUN
    
    END_RUN = options.ENDRUN
    
    REPRODUCE = options.REPRODUCE
    
    I3BUILD = options.I3BUILD
    if os.access(I3BUILD,os.R_OK) == False:
        raise RuntimeError("cannot access I3Build directory %s for reading!"%I3BUILD)
    
    PYTHONSCRIPTDIR = options.PythonScriptDir
    if os.access(PYTHONSCRIPTDIR,os.R_OK) == False:
        raise RuntimeError("cannot access directory containing python scripts"%PYTHONSCRIPT)
   
    logger.debug("start run: %s", START_RUN)
    logger.debug("end run  : %s", END_RUN)
    logger.debug("dryrun   : %s", str(dryrun))
    logger.debug("reproduce: %s", str(REPRODUCE))
    logger.debug("I3BUILD  : %s", I3BUILD)
    logger.debug("PyScripts: %s", PYTHONSCRIPTDIR)
 
    GRLInfo_ = dbs4_.fetchall("""SELECT r.run_id,r.tStart,g.production_version,g.snapshot_id
                              FROM i3filter.grl_snapshot_info_pass2 g
                              JOIN i3filter.run_info_summary_pass2 r
                                ON r.run_id=g.run_id
                              WHERE ((NOT g.GCDCheck AND NOT g.BadDOMsCheck AND NOT g.submitted) OR %s)
                                AND (g.good_it OR g.good_i3)
                                AND g.run_id>=%s
                                AND g.run_id<=%s""" % (REPRODUCE, START_RUN, END_RUN), UseDict = True)
    
    if not len(GRLInfo_):
        logger.warning("no runs meet input criteria for GCD generation ... exiting")
        exit(0)
    
    
    GRLInfo = {}
    for g in GRLInfo_:
        GRLInfo[g['run_id']] = [g['tStart'],g['production_version'],g['snapshot_id']]
    
    RunNums = GRLInfo.keys()
    RunNums.sort()
    
    for r in RunNums:
        logger.info("%s %s"%(r, GRLInfo[r]))
    
        StartDay = GRLInfo[r][0]
        PV = GRLInfo[r][1]
        SId = GRLInfo[r][2]
        
        Season = "IC86.%s_" % libs.config.get_season_by_run(int(r))

        sY = str(StartDay.year)
        sM = str(StartDay.month).zfill(2)
        sD = str(StartDay.day).zfill(2)
       
        GCDDirPass1 = "/data/exp/IceCube/%s/filtered/level2/OfflinePreChecks/DataFiles/%s%s"%(sY, sM, sD)
        GCDDir = "/data/exp/IceCube/%s/filtered/level2pass2/OfflinePreChecks/DataFiles/%s%s"%(sY, sM, sD)
        GCDDirAll = "/data/exp/IceCube/%s/filtered/level2pass2/AllGCD/" % sY
        GCDDirVerified = "/data/exp/IceCube/%s/filtered/level2pass2/VerifiedGCD/" % sY
 
        Clog = "/data/exp/IceCube/%s/filtered/level2pass2/OfflinePreChecks/run_logs/condor_logs/%s%s/"%(sY,sM,sD)
        Cerr = "/data/exp/IceCube/%s/filtered/level2pass2/OfflinePreChecks/run_logs/condor_err/%s%s/"%(sY,sM,sD)
        Olog = "/data/exp/IceCube/%s/filtered/level2pass2/OfflinePreChecks/run_logs/logs/%s%s/"%(sY,sM,sD)
        
        if not dryrun:
            if not os.path.exists(Clog):
                os.makedirs(Clog)
            if not os.path.exists(Cerr):
                os.makedirs(Cerr)
            if not os.path.exists(Olog):
                os.makedirs(Olog)
            if not os.path.exists(GCDDir):
                os.makedirs(GCDDir)
            if not os.path.exists(GCDDirAll):
                os.makedirs(GCDDirAll)
            if not os.path.exists(GCDDirVerified):
                os.makedirs(GCDDirVerified)
      
        GCDFilesPass1 = glob.glob(os.path.join(GCDDirPass1, "Level2_%sdata_Run00%s_*GCD.i3.gz" % (Season, r)))

        logger.debug("GCDFilesPass1 glob expression: %s" % os.path.join(GCDDirPass1, "Level2_%sdata_Run00%s_*GCD.i3.gz" % (Season, r)))
        logger.debug("GCDFilesPass1 = %s" % GCDFilesPass1)

        # Check for multiple GCD files
        if len(GCDFilesPass1) > 1:
            GCDFilesPass1.sort()
        elif len(GCDFilesPass1) == 0:
            logger.critical("Could not find input GCD file in %s" % GCDDirPass1)
            exit(1)
 
        GCDNamePass1 = GCDFilesPass1[-1]
        GCDName = os.path.join(GCDDir, "Level2pass2_%sdata_Run00%s_%s_%s_GCD.i3.gz" % (Season, r, PV, SId))
        GCDLinkName = "Level2pass2_%sdata_Run00%s_%s%s_%s_%s_GCD.i3.gz"%(Season, r, sM, sD, PV, SId)

        if not dryrun:
            os.system("ln -sf %s %s/%s"%(os.path.relpath(GCDName, GCDDirVerified), GCDDirVerified, GCDLinkName))
            os.system("ln -sf %s %s/%s"%(os.path.relpath(GCDName, GCDDirAll), GCDDirAll, GCDLinkName))

        SUBMITFILE = open(CONDOR_SUBMIT_FILE,"w")
        SUBMITFILE.write("Universe = vanilla ")
        SUBMITFILE.write('\nExecutable = %s/./env-shell.sh'%I3BUILD)
        SUBMITFILE.write("\narguments =  python -u %s/GCDGeneration_pass2.py %s %s %s %s %s %s "%(PYTHONSCRIPTDIR, GCDNamePass1, config.get('GCDGeneration', 'SpeCorrectionFile'), GCDName, r, PV, SId))
        SUBMITFILE.write("\nLog = %s/Run00%s_%s_%s.log"%(Clog,str(r),PV,SId))
        SUBMITFILE.write("\nError = %s/Run00%s_%s_%s.err"%(Cerr,str(r),PV,SId))
        SUBMITFILE.write("\nOutput = %s/Run00%s_%s_%s.out"%(Olog,str(r),PV,SId))
        SUBMITFILE.write("\nNotification = Never")
        #SUBMITFILE.write("\nRequestMemory = 4000")
        #SUBMITFILE.write("\nRequirements = TARGET.TotalCpus == 16")
        #SUBMITFILE.write("\nRequirements = TARGET.TotalCpus == 32")
        SUBMITFILE.write("\npriority = 15")
        #SUBMITFILE.write("\n+IsTestQueue = TRUE")
        #SUBMITFILE.write("\nrequirements = TARGET.IsTestQueue")
        SUBMITFILE.write('\ngetenv = True')
        SUBMITFILE.write("\nQueue")
        SUBMITFILE.close()
        ##
        if not dryrun:
            if REPRODUCE:
                logger.debug("Update grl_snapshot_info_pass2: GCDCheck = 0, BadDOMsCheck = 0, PoleGCDCheck = NULL, TemplateGCDCheck = NULL")
                dbs4_.execute("""UPDATE grl_snapshot_info_pass2
                                 SET GCDCheck = 0, BadDOMsCheck = 0, PoleGCDCheck = NULL, TemplateGCDCheck = NULL
                                 WHERE run_id = %s 
                                    AND snapshot_id = %s 
                                    AND production_version = %s
                              """ % (r, SId, PV))

            logger.debug("Execute `condor_submit %s`"%CONDOR_SUBMIT_FILE)
            processoutput = subprocess.check_output("condor_submit %s"%CONDOR_SUBMIT_FILE, shell = True, stderr=subprocess.STDOUT)
            logger.info(processoutput.strip())
        
