#!/usr/bin/env python

"""
This script takes all good runs that PoleGCDCheck has not been performed (yet).
It checks for these runs if the SPS GCD file actually exists.

Therefore, it helps to understand why the PoleGCDCheck is not performed for
some runs.
"""

from __future__ import print_function
import glob
import os
import stat
import subprocess
import SQLClient_dbs4 as dbs4
from libs.logger import get_logger
from libs.argparser import get_defaultparser

def CheckRun(runId, year, month, day, logger):
    """
    Checks if the SPS GCD file exists for this run.

    Args:
        runId (int): The Run Id
        year (int): Year of the run
        month (int): Month of the run
        day (int): Day of the run
        logger (logging.Logger): The logger
        verbose (bool): Be more verbose? Default is `True`

    Returns:
        bool: `True` if the SPS GCD file exists for this run. Otherwise, `False` is returned.
    """

    if month < 10:
        month = '0' + str(month)

    if day < 10:
        day = '0' + str(day)

    path = '/data/exp/IceCube/' + str(year) + '/internal-system/sps-gcd/' + str(month) + str(day) + '/SPS-GCD_Run*' + str(runId) + '*.i3.tar.gz';

    files = glob.glob(path)

    return len(files) > 0;

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
    LOGFILE=os.path.join(os.path.split(__file__)[0],"logs/PreProcessing/CheckForSPSGCDFiles_")
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

        if CheckRun(runId, date.year, date.month, date.day, logger):
            logger.info("Run %s has SPS-GCD file but has not been checked by PoleGCDCheck yet"%(runId))
        else:
            logger.info("No SPS-GCD file for run %s"%(runId))
            count += 1
    logger.info('---------------------------------')
    logger.info("%s runs have missing SPS-GCD file"%(count))
    
