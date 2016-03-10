#!/usr/bin/env python

"""
Calculates MD5 check sums of PFFilt files to accelerate job submission.
The calculated check sums are stored in a directory (FILE: MD5 sum) that
is dumped in cache file.

Usually this script is called by cron.
"""

import sys, os
from os.path import expandvars, join, exists
import glob
import cPickle
from datetime import date,datetime,timedelta
import subprocess as sub
import datetime
from libs.logger import get_logger
from libs.argparser import get_defaultparser

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
from RunTools import *
from FileTools import *

def main(logger, dryrun):
    lock_file = os.path.join(os.path.split(__file__)[0], "tmp/Chksums_Submit.lock")
    ChkSumCacheFile = os.path.join(os.path.split(__file__)[0], "IC86_2015.dat")

    logger.info("Attempting Update @ %s"%datetime.datetime.now().isoformat().replace("T"," "))

    # Check if this script is already running    
    if os.path.isfile(lock_file):
        f = open(lock_file ,'r')
        pid = f.readline()
        # Check if a process with this pid is still running, just printing the command w/o the ps header (so, no line if no process with PID is running)
        sub_proc = sub.Popen(['ps', '-p', str(pid), '-o', 'command='], shell=False, stdout=sub.PIPE)
        for line in sub_proc.stdout:
            # Check if the running process is still a PoleGCDCheck (is required since the PIDs are recycled)
            if 'CacheChksums_2015.py' in line:
                #print "Another instance of the Cache chksums script is running ... exiting"
                logger.info("Another instance of the Cache chksums script is running @ %s... exiting"%datetime.datetime.now().isoformat().replace("T"," "))
                exit(0)
    
        logger.info("removing stale lock file")
        os.system("rm -f " + lock_file)
    
    # Ok, it's not running. Lets store the current PID and proceed
    with open(lock_file ,'w') as f:
        f.write(str(os.getpid()))
    
    ChkSums = {}
    
    try:
        if os.path.isfile(ChkSumCacheFile):
            ChkSumsFile = open(ChkSumCacheFile,'rb')
            ChkSums = cPickle.load(ChkSumsFile)
            ChkSumsFile.close()
        else:
            os.system("touch %s "%(ChkSumCacheFile))
    except:
        logger.warn("could not open existing or new ChkSum cache file %s"%ChkSumCacheFile)
    
    current_day = date.today()
    look_back = current_day + timedelta(days=-14)
    dump_interval = 20
    dump_count = 0
    
    while look_back <= current_day:
        InDir = "/data/exp/IceCube/%s/filtered/PFFilt/%s%s"%(look_back.year,str(look_back.month).zfill(2),str(look_back.day).zfill(2))
        
        logger.debug('look_back = ' + str(look_back))
        logger.debug('InDir = ' + InDir)

        if os.access(InDir,os.R_OK):
            InFiles = glob.glob(InDir+"/PFFilt*.tar.bz2")
            InFiles.sort()
            
            for File in InFiles:
                if not File in ChkSums.keys():
                    try:
                        ChkSums[File] = str(FileTools(File).md5sum())
                        logger.info("md5sum('%s'): %s"%(File, ChkSums[File]))
                    except Exception, err:
                        logger.error("File: %s"%(File))
                        logger.error(str(err))

                        if not dryrun and dump_count>=dump_interval:
                            with open(ChkSumCacheFile,mode="r+") as f:
                                cPickle.dump(ChkSums,f)
                            dump_count = 0

                        dump_count+=1
        
        look_back+=timedelta(days=1)

    if not dryrun:    
        with open(ChkSumCacheFile,mode="r+") as f:
            cPickle.dump(ChkSums,f)
    
    if os.path.isfile(lock_file):
        logger.info("removing cache chksum submission lock file")
        os.system("rm -f " + lock_file)

if __name__ == "__main__":
    argparser = get_defaultparser(__doc__, dryrun = True)
    args = argparser.parse_args()

    LOGFILE=os.path.join(os.path.split(__file__)[0],"logs/PreProcessing/CacheChksums_")
    logger = get_logger(args.loglevel, LOGFILE)

    main(logger, args.dryrun)

