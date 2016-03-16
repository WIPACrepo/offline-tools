#!/usr/bin/env python

"""
Checks runs if they are affected by the leap second bug.
"""

import os
import SQLClient_dbs4 as dbs4
from libs.logger import get_logger
from libs.files import get_logdir
from libs.argparser import get_defaultparser
import glob
from icecube import icetray,dataclasses,dataio
from libs.times import ComputeTenthOfNanosec
from libs.checks import leap_second_affected_subruns, leap_second_affected_gcd
from RunTools import RunTools

if __name__ == '__main__':
    parser = get_defaultparser(__doc__)

    parser.add_argument('-s', '--startrun', type = int, required = True,
                        dest = "STARTRUN", 
                        help = "Start file check from this run")

    parser.add_argument("-e", "--endrun", type = int, required = True,
                        dest = "ENDRUN",
                        help = "Stop file check at this run")
    
    parser.add_argument("--season", type = int, required = True,
                        dest = "season",
                        help = "Season (e.g. 2015)")
    
    args = parser.parse_args()
 
    # Logger
    LOGFILE=os.path.join(get_logdir(), 'CheckIfLeapSecondAffected_')
    logger = get_logger(args.loglevel, LOGFILE)
	
    # Main script
    dbs4_ = dbs4.MySQL()

    if args.STARTRUN > args.ENDRUN:
        logger.error('The end run id must be equal or bigger than the start run id.')
        exit(1)
    
    runs = dbs4_.fetchall("""SELECT run_id, good_tStart, good_tStop, good_tStart_frac, good_tStop_frac, production_version FROM grl_snapshot_info 
                             WHERE  run_id BETWEEN %s AND %s 
                                    AND (good_i3 = 1 OR good_it = 1)"""%(args.STARTRUN, args.ENDRUN))

    if len(runs) == 0:
        logger.error("No runs found within the range of %s and %s."%(args.STARTRUN, args.ENDRUN))
        exit(1) 

    counter = 0
    bad_gcds = 0
    bad_start = 0
    bad_end = 0
    affected_start = []
    affected_end = []
    affected_gcds = []
    for run in runs:
        runId = run[0]
        good_tstart = run[1]
        good_tstop = run[2]
        good_tstart_frac = run[3]
        good_tstop_frac = run[4]
        production_version = run[5]

        gtstart =  dataclasses.I3Time(good_tstart.year,ComputeTenthOfNanosec(good_tstart, good_tstart_frac))
        gtstop = dataclasses.I3Time(good_tstop.year,ComputeTenthOfNanosec(good_tstop, good_tstop_frac))

        # Check GCDs
        affected = leap_second_affected_gcd(runId, gtstart, args.season, logger)
        bad_gcds += affected
        if affected:
            affected_gcds.append(runId)

        # Check subruns
        affected = leap_second_affected_subruns(runId, gtstart, gtstop, production_version, args.season, logger)

        if 'start' in affected:
            bad_start += 1
            affected_start.append(runId)

        if 'end' in affected:
            bad_end += 1
            affected_end.append(runId)

        counter += 1


    logger.info('------------------------------------------------------------------')
    logger.info("Checked %s GCD files. %s are affected by leap second bug:"%(counter, bad_gcds))

    for run in affected_gcds:
        logger.info("  %s"%run)

    logger.info('------------------------------------------------------------------')
    logger.info("Checked %s runs. %s mismatches in start times, %s possible missmatches in end time:"%(counter, bad_start, bad_end))
    logger.info('Start time mismatches:')
    for run in affected_start:
        logger.info("  %s"%run)

    logger.info('End time mismatches:')
    for run in affected_end:
        logger.info("  %s"%run)


