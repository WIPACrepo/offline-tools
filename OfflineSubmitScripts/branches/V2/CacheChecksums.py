#!/usr/bin/env python

"""
Calculates checksums of L2 input files in order to make the submission process faster.
"""

import os
import datetime
import time

from datetime import date, datetime, timedelta
from glob import glob

from libs.argparser import get_defaultparser
from libs.logger import get_logger
from libs.path import get_logdir
from libs.config import get_config
from libs.utils import DBChecksumCache
from libs.process import Lock
from libs.stringmanipulation import replace_var

def main(logger, args):
    config = get_config(logger)

    look_back_in_days = config.getint('CacheCheckSums', 'LookBack')
    hold_off_interval = config.getint('CacheCheckSums', 'HoldOffInterval')
    path_pattern = config.get_var_list('CacheCheckSums', 'Path')

    # Replace some vars
    path_pattern = [replace_var(p, 'run_id', '*') for p in path_pattern]
    path_pattern = [replace_var(p, 'sub_run_id', '*') for p in path_pattern]
    path_pattern = [replace_var(p, 'production_version', '*') for p in path_pattern]
    path_pattern = [replace_var(p, 'snapshot_id', '*') for p in path_pattern]

    # Cache certain dates
    if args.start_date is not None and args.end_date is not None:
        logger.info('** Cache checksums between start and end date **')

        from datetime import datetime
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.start_date, '%Y-%m-%d')

        logger.info('Start date: {0}'.format(start_date))
        logger.info('End date: {0}'.format(end_date))
    else:
        start_date = None
        end_date = None

    # Debug...
    logger.debug("LookBack in days: {0}".format(look_back_in_days))
    logger.debug("Hold-Off Interval: {0} seconds".format(hold_off_interval))
    logger.debug("Path pattern: {0}".format(path_pattern))

    if look_back_in_days < 0:
        logger.critical("Invalid value for LookBack: {0}".format(look_back_in_days))
        exit(1)

    logger.info("Attempting Update @ {0}".format(datetime.now().isoformat().replace("T"," ")))

    # Stop process if running
    lock = Lock(os.path.basename(__file__), logger)
    lock.lock()

    cache = DBChecksumCache(logger, dryrun = args.dryrun)

    current_day = date.today()
    look_back = current_day + timedelta(days = -look_back_in_days)

    if start_date is not None and end_date is not None:
        current_day = end_date.date()
        look_back = start_date.date()

    while look_back <= current_day:
        path = [p.format(year = look_back.year, month = look_back.month, day = look_back.day) for p in path_pattern]
        
        logger.debug('look_back = {0}'.format(look_back))
        logger.debug('path = {0}'.format(path))

        files = []
        for p in path:
            files.extend(glob(p))

        for path in files:
            logger.debug("Current file: {0}".format(path))

            if not cache.has_md5(path):
                logger.debug("File has no MD5 sum in cache")

                # Check if file is old enought to avoid MD5 sums of incompleted files
                last_mod = os.stat(path).st_mtime
                current_time = time.time()
                if current_time - last_mod < hold_off_interval:
                    # File is not old enough
                    logger.debug("File's last modification was at {0}. Its age is {1} seconds. Min age is {2} seconds.".format(last_mod, current_time - last_mod, hold_off_interval))
                    logger.debug('File is not old enough. Skip it.')
                    continue

                checksum = cache.set_md5(path)
                logger.info("md5('{0}'): {1}".format(path, checksum))

            else:
                logger.info('Already cached, skip file {0}'.format(path))

        look_back += timedelta(days = 1)

    lock.unlock()

if __name__ == "__main__":
    argparser = get_defaultparser(__doc__, dryrun = True)
    parser.add_argument('--start-date', type = str, required = False, default= None, help = "Do cache files between two dates. Start here. Format: YYYY-MM-DD")
    parser.add_argument('--end-date', type = str, required = False, default= None, help = "Do cache files between two dates. Stop here. Format: YYYY-MM-DD")
    args = argparser.parse_args()

    logfile=os.path.join(get_logdir(sublogpath = 'PreProcessing'), 'CacheChksums_')

    if args.logfile is not None:
        logfile = args.logfile

    logger = get_logger(args.loglevel, logfile)

    main(logger, args)

