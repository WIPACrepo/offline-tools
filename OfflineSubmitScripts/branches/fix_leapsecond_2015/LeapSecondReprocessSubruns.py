
"""
Creates a new run directory 'Run<RUN ID>_bad_subrun_leap_second' and copies the first
and last subrun of the current run into this directory.
"""

import LeapSecondCopyBadSubruns
import os
import sys
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir, get_tmpdir, GetGoodSubruns
import libs.times
from icecube import icetray, dataio, dataclasses
from I3Tray import *

import glob

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
import RunTools
import FileTools

sys.path.append("/data/user/i3filter/SQLServers_n_Clients/")
import SQLClient_dbs4 as dbs4

dbs4_ = dbs4.MySQL()

if __name__ == "__main__":
    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument('--l3muon', help="Repair L3 Muon",dest="l3muon",action="store_true",default=False)  
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(), 'LeapSecondReprocessSubruns_')
    logger = get_logger(args.loglevel,LOGFILE)

    dryrun = args.dryrun

    dataset = 1883
    if args.l3muon:
        dataset = 1885

    aff_last_subruns = LeapSecondCopyBadSubruns.read_runs_from_file('LeapSecondLastSubRunAffectedRuns.txt')
    logger.info("%s runs are affected by leap second for the last subrun"%len(aff_last_subruns))

    aff_first_subruns = LeapSecondCopyBadSubruns.read_runs_from_file('LeapSecondFirstSubRunAffectedRuns.txt')
    logger.info("%s runs are affected by leap second for the first subrun"%len(aff_first_subruns))

    # Build set
    aff_runs = set(aff_last_subruns).union( set(aff_first_subruns))

    # Build sql string
    sql_runs = ','.join(aff_runs)

    # Sql query 
    sql = """SELECT r.run_id, MIN(sub_run) AS first_sr, MAX(sub_run) AS last_sr 
                FROM run r 
                JOIN job j 
                    ON r.queue_id = j.queue_id 
                    AND r.dataset_id = j.dataset_id 
                JOIN grl_snapshot_info g 
                    ON r.run_id = g.run_id
                WHERE r.dataset_id = %s
                    AND r.run_id > 126539
                    AND r.run_id IN (%s) 
                    AND (good_i3 = 1 OR good_it = 1) 
                GROUP BY r.run_id 
                ORDER BY r.run_id""" % (dataset, sql_runs)


    info = dbs4_.fetchall(sql, UseDict = True)
  
    logger.debug("First subrun affected runs: %s"%str(aff_first_subruns)) 
    logger.debug("Last subrun affected runs: %s"%str(aff_last_subruns)) 
 
    logger.debug("Dict: %s"%str(info))

    for run in info:
        run_id = run['run_id']
        
        subruns = ''
        if str(run_id) in aff_first_subruns:
            subruns = str(run['first_sr'])

        if str(run_id) in aff_last_subruns:
            if len(subruns):
                subruns = subruns + ',' + str(run['last_sr'])
            else:
                subruns = str(run['last_sr'])

        if args.l3muon:
            sqlurlpath = """SELECT path, name, run_id, md5sum, date, urlpath_id, transferstate
                            FROM urlpath u 
                            JOIN run r 
                                ON r.queue_id = u.queue_id 
                                AND u.dataset_id = r.dataset_id 
                            WHERE run_id = %s
                                AND path LIKE 'file:/data/exp/IceCube/____/filtered/level2/____/Run%%' 
                                AND r.dataset_id = %s
                                AND sub_run IN (%s)
                            ORDER BY name ASC""" % (run['run_id'], dataset, subruns)

            urlp = dbs4_.fetchall(sqlurlpath, UseDict = True)
            urlp_first_sr = urlp[0]
            urlp_last_sr = urlp[-1]

        sql = """UPDATE job j
                    JOIN run r
                        ON r.queue_id = j.queue_id
                        AND r.dataset_id = j.dataset_id
                SET status = 'WAITING'
                WHERE run_id = %s
                AND r.dataset_id = %s
                AND sub_run IN (%s)"""%(run_id, dataset, subruns)

        if not args.l3muon:
            sql2 = "UPDATE grl_snapshot_info SET validated = 0 WHERE run_id = %s"%run_id

        logger.info("Run %s"%run_id)
        logger.info("Execute sql: %s"%sql)

        if args.l3muon:
            # Need to update md5 sum of input file
            
            if str(run_id) in aff_first_subruns:
                md5_first_sr = FileTools.FileTools(os.path.join(urlp_first_sr['path'][5:], urlp_first_sr['name']), logger).md5sum()
                size = os.path.getsize(os.path.join(urlp_first_sr['path'][5:], urlp_first_sr['name']))
                sql3 = "UPDATE urlpath SET md5sum = '%s', size = %s WHERE urlpath_id = %s" % (md5_first_sr, size, urlp_first_sr['urlpath_id'])
                logger.info("Execute sql: %s" % sql3)

                if not dryrun:
                    dbs4_.execute(sql3)

            if str(run_id) in aff_last_subruns:
                md5_last_sr = FileTools.FileTools(os.path.join(urlp_last_sr['path'][5:], urlp_last_sr['name']), logger).md5sum()
                size = os.path.getsize(os.path.join(urlp_last_sr['path'][5:], urlp_last_sr['name']))
                sql3 = "UPDATE urlpath SET md5sum = '%s', size = %s WHERE urlpath_id = %s" % (md5_last_sr, size, urlp_last_sr['urlpath_id'])
                logger.info("Execute sql: %s" % sql3)

                if not dryrun:
                    dbs4_.execute(sql3)

        if not args.l3muon:
            logger.info("Execute sql: %s"%sql2)

        if not dryrun:
            dbs4_.execute(sql)

            if not args.l3muon:
                dbs4_.execute(sql2)

