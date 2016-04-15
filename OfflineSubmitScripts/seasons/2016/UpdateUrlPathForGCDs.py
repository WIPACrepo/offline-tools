#!/usr/bin/env python

"""
Updates the urlpath table if you reprocessed already in urlpath inserted GCD files.
"""

import SQLClient_dbs4
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir
import glob
import os
import sys

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
from FileTools import *

if __name__ == "__main__":
    parser = get_defaultparser(__doc__,dryrun=True)

    parser.add_argument("-s", "--startrun", type=int, required = True, default=-1,
                      dest="STARTRUN", help="Start updateing urlpath from this run")
    
    parser.add_argument("-e", "--endrun", type=int, required = True, default=-1,
                      dest="ENDRUN", help="End updateing urlpath at this run")

    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(), 'UpdateUrlPathForGCDs_')
    logger = get_logger(args.loglevel,LOGFILE)

    if args.STARTRUN > args.ENDRUN:
        logger.critical("Start run (%s) must be equal or smaller than endrun (%s)"%(args.STARTRUN, args.ENDRUN))
        exit(1)

    sql = """SELECT path, name, run_id, md5sum, date, urlpath_id, transferstate
             FROM urlpath u 
             JOIN run r ON r.queue_id = u.queue_id AND u.dataset_id = r.dataset_id 
             WHERE run_id BETWEEN %s AND %s
                AND path LIKE 'file:/data/exp/IceCube/____/filtered/level2/VerifiedGCD/'"""%(args.STARTRUN, args.ENDRUN)
    
    dbs4 = SQLClient_dbs4.MySQL()
    runs = dbs4.fetchall(sql, UseDict = 1)

    sumcache = {}

    for run in runs:
            gcddir = "/data/exp/IceCube/%s/filtered/level2/OfflinePreChecks/DataFiles/%s%s/"%(str(run['date'].year), str(run['date'].month).zfill(2), str(run['date'].day).zfill(2))

            gcdpath = "%sLevel2_IC86.2015_data_Run%s_*_GCD.i3.gz"%(gcddir, str(run['run_id']).zfill(8))

            file = glob.glob(gcdpath)

            logger.debug("Created GCD path: %s"%gcdpath)

            if not len(file):
                logger.error("No GCD file found for run %s"%str(run['run_id']))
                continue

            logger.info("Found %s GCD files for run %s."%(len(file), run['run_id']))
            
            file.sort()

            file = file[-1];

            if file in sumcache:
                md5sum = sumcache[file]
            else:
                md5sum = FileTools(file).md5sum()
                sumcache[file] = md5sum

            size = os.path.getsize(file)
    
            logger.debug("File %s"%file)
            logger.debug("    MD5: %s"%md5sum)

            if md5sum == run['md5sum']:
                logger.warning("MD5 sum didn't change for run %s"%run['run_id'])
                logger.warning('No update will be performed for this run')
            else:
                sql = "UPDATE urlpath SET transferstate = 'WAITING', md5sum = '%s', size = %s, verify = 0 WHERE urlpath_id = %s"%(md5sum, size, str(run['urlpath_id']))
                logger.debug("Execute sql update: %s"%sql)

                if not args.dryrun:
                    dbs4.execute(sql)

                logger.info("Run %s updated: MD5SUM %s -> %s, TRANSFERSTATE %s -> %s"%(run['run_id'], run['md5sum'], md5sum, run['transferstate'], 'WAITING'))
