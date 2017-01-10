#!/usr/bin/env python
"""
Compare the GCD Files created in the North to those created
at the Pole and transferred to the North
"""

import os, sys
import time
import datetime
import glob
import json
import subprocess as sub
from libs.files import get_tmpdir, get_logdir
import libs.config

CONFIG = libs.config.get_config()

sys.path.append(CONFIG.get('DEFAULT', 'SQLClientPath'))
sys.path.append(CONFIG.get('DEFAULT', 'ProductionToolsPath'))

from RunTools import *
from FileTools import *
from DbTools import *
from libs.logger import get_logger
from libs.argparser import get_defaultparser
import libs.process

import SendNotification as SN

import SQLClient_dbs4 as dbs4
dbs4_ = dbs4.MySQL()

DEFAULT_START_RUN = CONFIG.get('PoleGCDChecks', 'DefaultStartRun')
ENVSHELL   = "%s/./env-shell.sh" % CONFIG.get('L2', 'I3_BUILD')
OFFLINEPRODUCTIONTOOLS = CONFIG.get('DEFAULT', 'ProductionToolsPath')
VERIFIEDGCD = CONFIG.get('PoleGCDChecks', 'VerifiedGCDsPath')

CMPGCD = CONFIG.get('PoleGCDChecks', 'CmpGCDScriptName')
SENDER = CONFIG.get('Notifications', 'eMailSender')
RECEIVERS = libs.config.get_var_list('PoleGCDChecks', 'NotificationReceiver')

DOMAIN = CONFIG.get('Notifications', 'eMailDomain')
LOGFILEPATH = get_logdir(sublogpath="PoleGCDChecks")
LOGFILE = os.path.join(LOGFILEPATH,"PoleGCDChecks_")

def main(logger, StartRun = DEFAULT_START_RUN, dryrun=False):
    # default run number to start checks, this can be over-written by supplying an
    # no valid GCD files between season start (126378) and 126444
    # run when 'good' GCD files started flowing again from Pole (126445)
    runs_ = dbs4_.fetchall("""SELECT * FROM i3filter.grl_snapshot_info_pass2 g
                 where (good_it or good_i3 or run_id IN (127891, 127892, 127893)) and run_id>=%d
                 and PoleGCDCheck is NULL order by run_id """%StartRun, UseDict=True)
    
    run_id = [r['run_id'] for r in runs_]
  
    logger.info("Attempt to check %s runs" % len(run_id))
 
    first_season = libs.config.get_season_by_run(StartRun)
    last_season = libs.config.get_season_by_run(int(sorted(run_id)[-1]))

    logger.debug("First run/season: %s/%s" % (StartRun, first_season))
    logger.debug("last run/season: %s/%s" % (int(sorted(run_id)[-1]), last_season))
 
    # Loop over all affected seasons
    inDirs = []
    for year in range(first_season, last_season + 2):
        folder = "/data/exp/IceCube/%s/internal-system/sps-gcd" % year

        logger.debug("Add folder %s for SPS GCD files" % folder);

        inDirs.extend(glob.glob(folder + "/*"))
    
    if not len(inDirs):
        logger.info("No GCD file meet criteria for testing .... exiting")
        exit(0)
        
    inDirs.sort()

    # Find files
    Files = []
    for i in inDirs:
        if os.path.isdir(i):
            Files.extend(glob.glob(i+"/*GCD*"))
            Files.sort()

    # Map run id and file
    files_map = {}
    for f in Files:
        try:
            runNum = int(f.split('.i3.tar.gz')[0][-6:])
            files_map[runNum] = f
        except:
            logger.warning("could not extract RunNumber from %s" %f)
                
    # Check runs
    for runNum in run_id:
        if runNum not in files_map:
            logger.warning("Run %s has no SPS GCD file" % runNum)
            continue

        f = files_map[int(runNum)]

        sub.check_call(["tar","xvzf",f])
        tFile = os.path.basename(f).replace(".tar.gz",".dat.tar")
        sub.check_call(["tar","xvf",tFile])
        zFile = os.path.basename(f).replace(".i3.tar.gz",".i3.gz")
        sub.check_call(["gunzip",zFile])
        poleFile = os.path.basename(f).replace(".i3.tar.gz",".i3")
        
        logger.debug("Working with file %s" %poleFile)
        
        year_ = f[f.find("IceCube/")+8:f.find("/internal-system")]
        run_ = f[f.find("_Run")+4:f.find(".i3.tar.gz")]
        if not run_.isdigit(): run_ = f[f.find("_run")+4:f.find(".i3.tar.gz")]
        if not run_.isdigit():
            logger.warning( " could not get run number from Pole file name ...")
            continue
      
        logger.debug(VERIFIEDGCD) 
        logger.debug("Search for verified GCD file with %s" % (VERIFIEDGCD.replace('%%s', '%s') % year_ + "/*" + str(run_) + "*")) 
        northFile = glob.glob(VERIFIEDGCD.replace('%%s', '%s') % year_ + "/*" + str(run_) + "*")
        if not len(northFile):
            logger.warning(" **** no Verified GCD file in the north for run %s ****"%run_)
            #clean up
            files = glob.glob("*" + str(run_) + "*")
            logger.debug("Removing files %s" %files.__repr__())
            map(os.remove,files)
            continue
    
        logger.debug("North file(s): %s" % northFile)
        logger.debug("Pole file: %s" % poleFile)
    
        if len(northFile)>0:northFile.sort(key=lambda x: os.path.getmtime(x),reverse=True)
        northFile = northFile[0]
        sub.check_call(["cp",northFile,"."])
        outLog = os.path.join(LOGFILEPATH,"Run%s.logs"%run_)
        
        with open (outLog,"w") as oL:
            try:
                RV = sub.call([ENVSHELL,
                                "python", os.path.join(OFFLINEPRODUCTIONTOOLS,CMPGCD),
                                "-f", "%s" % northFile, "%s" % poleFile,"-v"],stdout=oL, stderr=oL)
                
                if not dryrun: dbs4_.execute("""update i3filter.grl_snapshot_info_pass2 g
                                 set PoleGCDCheck=%s where run_id=%s"""%(RV,runNum))
                
                if RV:
                    message = ""
                    #receivers = ['ofadiran@icecube.wisc.edu']
                    subject = " Pole/North GCD check for Run%s"%runNum
                    messageBody = ""

                    # only necessary for html emails
                    mimeVersion="1.0"
                    contentType="text/html"

                    messageBody += """
                        *** This is an automated message generated by the *** <br>
                        ***        Pole/North GCD Comparison System   *** <br><br>
          
                        GCD Pole/North Comparison Check for<br>
                        Run:<b>%s</b><br>
                        returned a non-zero value: <b>%s</b> <br>
                        The files compared are: <br>
                        northFile: %s <br>
                        poleFile: %s
                        """%(runNum,RV,northFile,f)

                    message = SN.CreateMsg(DOMAIN, SENDER, RECEIVERS, subject,messageBody,mimeVersion,contentType)

                    if len(message) and not dryrun:
                        SN.SendMsg(SENDER,RECEIVERS,message)

                    logger.info("Check failed for run %s" % runNum)
                else:
                    logger.info("Run %s passed check" % runNum)

            except Exception, err:
                oL.write("\n Error for run %s"%run_)
                oL.write(str(err))
        #clean up
        try: 
            os.system("rm *%s*" %run_)
        except OSError as e:
            logger.exception("Problems removing *%s*, exception %s" %(run, e.__repr__()))              

if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument('-s', '--startrun', type = int, default = DEFAULT_START_RUN,
                        dest = "STARTRUN",
                        help = "Start run check from this run")
    args = parser.parse_args()
    logger = get_logger(args.loglevel, LOGFILE)   

    logger.info( "Attempting PoleGCDChecks @ %s"%datetime.datetime.now().isoformat().replace("T"," "))    
   
    lock = libs.process.Lock(os.path.basename(__file__), logger)
    lock.lock()

    logger.debug("Notification Receivers: %s" % RECEIVERS)
    logger.debug("Sender: %s%s" % (SENDER, DOMAIN))

    main(logger,StartRun = args.STARTRUN,dryrun=args.dryrun)
    
    lock.unlock()
