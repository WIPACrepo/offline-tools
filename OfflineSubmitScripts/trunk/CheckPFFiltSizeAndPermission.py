#!/usr/bin/env python

"""
Checks for the given runs the PFFilt files if they have a file sizes > 0 and have proper file permissions.
"""

import os
from libs.logger import get_logger
from libs.argparser import get_defaultparser
import libs.checks
import SQLClient_dbs4 as dbs4

if __name__ == '__main__':
    parser = get_defaultparser(__doc__)

    parser.add_argument('-s', '--startrun', type = int, required = True,
                        dest = "STARTRUN", 
                        help = "Start file check from this run")

    parser.add_argument("-e", "--endrun", type = int, required = True,
                        dest = "ENDRUN",
                        help = "Stop file check at this run")
    
    args = parser.parse_args()
    
    LOGFILE=os.path.join(os.path.split(__file__)[0],"logs/PreProcessing/CheckPFFiltSizeAndPermission_")
    logger = get_logger(args.loglevel, LOGFILE)

    dbs4_ = dbs4.MySQL()

    if args.STARTRUN > args.ENDRUN:
        logger.error('The end run id must be equal or bigger than the start run id.')
        exit(1)

    runs = dbs4_.fetchall("""SELECT run_id, tStart FROM run_info_summary 
                             WHERE run_id BETWEEN %s AND %s"""%(args.STARTRUN, args.ENDRUN))

    if len(runs) == 0:
        logger.error("No runs found within the range of %s and %s."%(args.STARTRUN, args.ENDRUN))
        exit(1) 

    paths = {}

    for run in runs:
        date = run[1]
        runId = run[0]

        month = str(date.month)
        day = str(date.day)

        if date.month < 10: month = '0' + month
        if date.day < 10: day = '0' + day

        paths[runId] = '/data/exp/IceCube/' + str(date.year) + '/filtered/PFFilt/' + month + day + '/PFFilt_*' + str(runId) + '*.tar.bz2'

    emptyFiles = []
    wrongPermissions = []
    emptyAndWPerm = []

    logger.info('Attempt to check files for ' + str(len(paths)) + ' runs')

    for run in runs:
        runId = run[0]
        date = run[1]

        result = libs.checks.pffilt_size_and_permission(runId, date.year, date.month, date.day, logger)
        emptyFiles += result['empty']
        wrongPermissions += result['permission']
        emptyAndWPerm += result['emptyAndPermission']

    logger.info('')
    logger.info('---------------------------------------------')
    logger.info('                  REPORT:')
    logger.info('---------------------------------------------')
    logger.info('---------------------------------------------')
    logger.info('EMPTY FILES W/ READING PERMISSION (' + str(len(emptyFiles)) + ')')
    logger.info('---------------------------------------------')
    for file in emptyFiles:
        logger.info(file)

    logger.info('')
    logger.info('---------------------------------------------')
    logger.info('EMPTY FILES W/O READING PERMISSION (' + str(len(emptyAndWPerm)) + ')')
    logger.info('---------------------------------------------')
    for file in emptyAndWPerm:
        logger.info(file)

    logger.info('')
    logger.info('---------------------------------------------')
    logger.info('NOT EMPTY FILES W/O READING PERMISSION (' + str(len(wrongPermissions)) + ')')
    logger.info('---------------------------------------------')
    for file in wrongPermissions:
        logger.info(file)

