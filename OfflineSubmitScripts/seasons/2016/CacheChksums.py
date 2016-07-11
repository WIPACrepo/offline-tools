#!/usr/bin/env python

"""
Calculates MD5 check sums of PFFilt files to accelerate job submission.
The calculated check sums are stored in a directory (FILE: MD5 sum) that
is dumped in cache file.

Usually this script is called by cron.
"""

import sys, os
import time
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
import libs.config


def main(logger, dryrun):
    config = libs.config.get_config()

    sys.path.append(config.get('DEFAULT', 'ProductionToolsPath'))
    import RunTools
    import FileTools

    ChkSumCacheFile = config.get('CacheCheckSums', 'CacheFile')

    look_back_in_days = config.getint('CacheCheckSums', 'LookBack')
    dump_interval = config.getint('CacheCheckSums', 'DumpInterval')
    hold_off_interval = config.getint('CacheCheckSums', 'HoldOffInterval')
    
    logger.debug("LookBack in days: %s" % look_back_in_days)
    logger.debug("DumpInterval: %s" % dump_interval)
    logger.debug("Hold-Off Interval: %s seconds" % hold_off_interval)

    if look_back_in_days < 0:
        logger.critical("Invalid value for LookBack: %s" % look_back_in_days)
        exit(1)

    if dump_interval < 1:
        logger.critical("Invalid value for DumpInterval: %s" % dump_interval)
        exit(1)

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
    look_back = current_day + timedelta(days=-look_back_in_days)
    dump_count = 0
    
    while look_back <= current_day:
        InDir = "/data/exp/IceCube/%s/filtered/PFFilt/%s%s"%(look_back.year,str(look_back.month).zfill(2),str(look_back.day).zfill(2))
        
        logger.debug('look_back = ' + str(look_back))
        logger.debug('InDir = ' + InDir)

        if os.access(InDir,os.R_OK):
            InFiles = glob.glob(InDir+"/PFFilt*.tar.bz2")
            InFiles.sort()
            
            for File in InFiles:
                logger.debug("Current file: %s"%File)

                if not File in ChkSums.keys():
                    logger.debug("File has no MD5 sum in cache")

                    try:
                        # Check if file is old enought to avoid MD5 sums of incompleted files
                        last_mod = os.stat(File).st_mtime
                        current_time = time.time()
                        if current_time - last_mod < hold_off_interval:
                            # File is not old enough
                            logger.debug("File's last modification was at %s. Its age is %s seconds. Min age is %s seconds." % (last_mod, current_time - last_mod, hold_off_interval))
                            logger.debug('File is not old enough. Skip it.')
                            continue

                        ChkSums[File] = str(FileTools.FileTools(File, logger).md5sum())
                        logger.info("md5sum('%s'): %s"%(File, ChkSums[File]))
                    except Exception, err:
                        logger.error("File: %s"%(File))
                        logger.error(str(err))

                    logger.debug("Calculated MD5 sum")

                    if not dryrun and dump_count>=dump_interval:
                        logger.debug("Write cache")

                        with open(ChkSumCacheFile,mode="r+") as f:
                            cPickle.dump(ChkSums,f)
                        dump_count = 0

                    dump_count+=1
        
        look_back+=timedelta(days=1)

    if not dryrun:
        logger.debug("Write last MD% sums")
        with open(ChkSumCacheFile,mode="r+") as f:
            cPickle.dump(ChkSums,f)

    lock.unlock()    

if __name__ == "__main__":
    argparser = get_defaultparser(__doc__, dryrun = True)
    args = argparser.parse_args()

    LOGFILE=os.path.join(get_logdir(sublogpath = 'PreProcessing'), 'CacheChksums_')
    logger = get_logger(args.loglevel, LOGFILE)

    main(logger, args.dryrun)

