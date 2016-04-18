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
from RunTools import *
from FileTools import *
from DbTools import *
from libs.logger import get_logger
from libs.argparser import get_defaultparser
import libs.process
import libs.config

CONFIG = libs.config.get_config()

sys.path.append(CONFIG.get('DEFAULT', 'SQLClientPath'))
sys.path.append(CONFIG.get('DEFAULT', 'ProductionToolsPath'))

import SendNotification as SN

import SQLClient_dbs4 as dbs4
dbs4_ = dbs4.MySQL()

SEASON = CONFIG.getint('DEFAULT', 'Season')
DEFAULT_START_RUN = CONFIG.get('PoleGCDChecks', 'DefaultStartRun')
INDIR_FIRST = "/data/exp/IceCube/%s/internal-system/sps-gcd" % SEASON
INDIR_SECOND = "/data/exp/IceCube/%s/internal-system/sps-gcd" % (SEASON + 1)
ENVSHELL   = "%s/./env-shell.sh" % CONFIG.get('DEFAULT', 'I3_BUILD')
OFFLINEPRODUCTIONTOOLS = CONFIG.get('DEFAULT', 'ProductionToolsPath')
DATAPATH = '/data/exp/IceCube'
VERIFIEDGCD = CONFIG.get('PoleGCDChecks', 'VerifiedGCDsPath')

CMPGCD = CONFIG.get('PoleGCDChecks', 'CmpGCDScriptName')
SENDER = CONFIG.get('Notifications', 'eMailSender')
RECEIVERS = json.loads(CONFIG.get('PoleGCDChecks', 'NotificationReceivers'))

DOMAIN = CONFIG.get('Notifications', 'eMailDomain')
LOGFILEPATH = get_logdir(sublogpath="PoleGCDChecks")
LOGFILE = os.path.join(LOGFILEPATH,"PoleGCDChecks_")

def main(logger, StartRun = DEFAULT_START_RUN, dryrun=False):
    # default run number to start checks, this can be over-written by supplying an
    # no valid GCD files between season start (126378) and 126444
    # run when 'good' GCD files started flowing again from Pole (126445)
    runs_ = dbs4_.fetchall("""SELECT * FROM i3filter.grl_snapshot_info g
                 where (good_it or good_i3) and run_id>=%d
                 and PoleGCDCheck is NULL order by run_id """%StartRun, UseDict=True)
    
    run_id = [r['run_id'] for r in runs_]
    
    inDir = INDIR_FIRST
    inDirs = glob.glob(INDIR_FIRST + "/*")
    if os.path.isdir(INDIR_SECOND):
        inDirs.extend(glob.glob(INDIR_SECOND + "/*"))
    
    if not len(inDirs):
        logger.info("No GCD file meet criteria for testing .... exiting")
        exit(0)
        
    inDirs.sort()
    for i in inDirs:
        if os.path.isdir(i):
            Files = glob.glob(i+"/*GCD*")
            Files.sort()
            for f in Files:
                try:
                    runNum = int(f.split('.i3.tar.gz')[0][-6:])
                except:
                    logger.warning("could not extract RunNumber from %s" %f)
                    continue
                
                if not runNum in run_id: continue
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
                
                northFile = glob.glob(DATAPATH + str(year_) + "/" + VERIFIEDGCD + "/*" + str(run_) + "*")
                if not len(northFile):
                    logger.warning(" **** no Verified GCD file in the north for run %s ****"%run_)
                    #clean up
                    files = glob.glob("*" + str(run_) + "*")
                    logger.debug("Removing files %s" %files.__repr__())
                    map(os.remove,files)
                    continue
                
                if len(northFile)>0:northFile.sort(key=lambda x: os.path.getmtime(x),reverse=True)
                northFile = northFile[0]
                sub.check_call(["cp",northFile,"."])
                outLog = os.path.join(LOGFILEPATH,"Run%s.logs"%run_)
                
                with open (outLog,"w") as oL:
                    try:
                        RV = sub.call([ENVSHELL,
                                        "python", os.path.join(OFFLINEPRODUCTIONTOOLS,CMPGCD),
                                        "-f", "%s %s"%(northFile,poleFile),"-v"],stdout=oL, stderr=oL)
                        
                        if not dryrun: dbs4_.execute("""update i3filter.grl_snapshot_info g
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

    logger.debug("INDIR_FIRST = %s" % INDIR_FIRST)
    logger.debug("INDIR_SECOND = %s" % INDIR_SECOND)

    logger.info( "Attempting PoleGCDChecks @ %s"%datetime.datetime.now().isoformat().replace("T"," "))    
   
    lock = libs.process.Lock(os.path.basename(__file__), logger)
    lock.lock()

    main(logger,StartRun = args.STARTRUN,dryrun=args.dryrun)
    
    lock.unlock()
