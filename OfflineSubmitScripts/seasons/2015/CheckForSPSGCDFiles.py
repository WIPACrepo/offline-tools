#!/usr/bin/env python

"""
This script takes all good runs that PoleGCDCheck has not been performed (yet).
It checks for these runs if the SPS GCD file actually exists.

Therefore, it helps to understand why the PoleGCDCheck is not performed for
some runs.
"""

import glob
import os
import stat
import subprocess
import SQLClient_dbs4 as dbs4
from libs.logger import get_logger
from libs.files import get_logdir
from libs.argparser import get_defaultparser
import libs.checks

if __name__ == '__main__':
    # Handle arguments
    parser = get_defaultparser(__doc__)

    parser.add_argument('-s', '--startrun', type = int, required = True,
                        dest = "STARTRUN", 
                        help = "Start file check from this run")

    parser.add_argument("-e", "--endrun", type = int, required = True,
                        dest = "ENDRUN",
                        help = "Stop file check at this run")
    
    args = parser.parse_args()
 
    # Logger
    LOGFILE=os.path.join(get_logdir(sublogpath = 'PreProcessing'), 'CheckForSPSGCDFiles_')
    logger = get_logger(args.loglevel, LOGFILE)
	
    # Main script
    dbs4_ = dbs4.MySQL()

    if args.STARTRUN > args.ENDRUN:
        logger.error('The end run id must be equal or bigger than the start run id.')
        exit(1)

    runs = dbs4_.fetchall("""SELECT run_id, good_tStart FROM grl_snapshot_info 
                             WHERE  run_id BETWEEN %s AND %s 
                                    AND (good_i3 = 1 OR good_it = 1) AND PoleGCDCheck IS NULL"""%(args.STARTRUN, args.ENDRUN))

    if len(runs) == 0:
        logger.error("No runs found within the range of %s and %s that have not been checked by the PoleGCDCheck."%(args.STARTRUN, args.ENDRUN))
        exit(1) 

    logger.info('Attempt to check files for ' + str(len(runs)) + ' runs')
    logger.info('---------------------------------')

    count = 0
    for run in runs:
        runId = run[0]
        date = run[1]

        if libs.checks.has_sps_gcd_file(runId, date.year, date.month, date.day, logger):
            logger.info("Run %s has SPS-GCD file but has not been checked by PoleGCDCheck yet"%(runId))
        else:
            logger.info("No SPS-GCD file for run %s"%(runId))
            count += 1
    logger.info('---------------------------------')
    logger.info("%s runs have missing SPS-GCD file"%(count))
    
