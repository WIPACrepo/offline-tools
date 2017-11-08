#!/usr/bin/env python

"""
Calculates checksums of L2 input files in order to make the submission process faster.
"""

import os
import time

from datetime import date, datetime, timedelta
from glob import glob

from libs.argparser import get_defaultparser
from libs.logger import get_logger
from libs.path import get_logdir
from libs.config import get_config
from libs.utils import DBChecksumCache
from libs.stringmanipulation import replace_var

def main(logger, args):
    config = get_config(logger)

    look_back_in_days = config.getint('CacheCheckSums', 'LookBack')
    hold_off_interval = config.getint('CacheCheckSums', 'HoldOffInterval')

    if args.type == 'config':
        path_pattern = config.get_var_list('CacheCheckSums', 'Path')
    else:
        path_pattern = [config.get(args.type, args.type + 'File')]

    if args.path_pattern is not None:
        path_pattern = [args.path_pattern]

    # Replace some vars
    path_pattern = [replace_var(p, 'run_id', '*') for p in path_pattern]
    path_pattern = [replace_var(p, 'sub_run_id', '*') for p in path_pattern]
    path_pattern = [replace_var(p, 'production_version', '*') for p in path_pattern]
    path_pattern = [replace_var(p, 'snapshot_id', '*') for p in path_pattern]

    # Cache certain dates
    if args.start_date is not None and args.end_date is not None:
        logger.info('** Cache checksums between start and end date **')

        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

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

    cache = DBChecksumCache(logger, dryrun = args.dryrun)

    current_day = date.today()
    look_back = current_day + timedelta(days = -look_back_in_days)

    if start_date is not None and end_date is not None:
        current_day = end_date.date()
        look_back = start_date.date()

    logger.debug('Current Day: {0}'.format(current_day))

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

if __name__ == "__main__":
    argparser = get_defaultparser(__doc__, dryrun = True)
    argparser.add_argument('--start-date', type = str, required = False, default= None, help = "Do cache files between two dates. Start here. Format: YYYY-MM-DD")
    argparser.add_argument('--end-date', type = str, required = False, default= None, help = "Do cache files between two dates. Stop here. Format: YYYY-MM-DD")
    argparser.add_argument('--type', type = str, required = False, default= 'config', help = "Type of file that should be chached: config, Level2, PFFilt, PFDST. Default is config. Config means that the paths will be read from the config file.")
    argparser.add_argument('--path-pattern', type = str, required = False, default= None, help = "Use this path pattern and not the ones from the config file. You can use {year}, {month}, {day}, {run_id}, {sub_run_id}, etc. as placeholder.")
    parser.add_argument("--cron", action = "store_true", default = False, help = "Use this option if you call this script via a cron")
    args = argparser.parse_args()

    logfile=os.path.join(get_logdir(sublogpath = 'PreProcessing'), 'CacheChksums_')

    if args.logfile is not None:
        logfile = args.logfile

    logger = get_logger(args.loglevel, logfile, svn_info_from_file = True)

    if args.cron:
        if not config.getboolean('CacheCheckSums', 'CronEnabled'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

    if args.type not in ['config', 'Level2', 'PFDST', 'PFFilt']:
        logger.critical('Unknown file type')
        exit(1)

    main(logger, args)

