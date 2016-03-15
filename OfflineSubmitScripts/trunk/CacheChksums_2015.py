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
from libs.files import get_logdir, get_rootdir
import libs.process

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
from RunTools import *
from FileTools import *

def main(logger, dryrun):
    # FIXME: adjust paths for season
    ChkSumCacheFile = os.path.join(get_rootdir(), "IC86_2015.dat")

    logger.debug("Cache file for check sums: %s"%ChkSumCacheFile)

    logger.info("Attempting Update @ %s"%datetime.datetime.now().isoformat().replace("T"," "))

    # Stop process if running
    lock = libs.process.Lock(os.path.basename(__file__), logger)
    lock.lock()    

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

    lock.unlock()    

if __name__ == "__main__":
    argparser = get_defaultparser(__doc__, dryrun = True)
    args = argparser.parse_args()

    LOGFILE=os.path.join(get_logdir(sublogpath = 'PreProcessing'), 'CacheChksums_')
    logger = get_logger(args.loglevel, LOGFILE)

    main(logger, args.dryrun)

