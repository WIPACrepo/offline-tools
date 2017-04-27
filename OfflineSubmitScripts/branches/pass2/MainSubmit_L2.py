#!/usr/bin/env python

"""
Creates PFFilt entries in 'url_path', 'jobs', and 'run' tables in dbs4
to be used by IceProd in creating/submitting L2 jobs.
"""

import sys, os
from os.path import expandvars, join, exists
import glob
from optparse import OptionParser
import time
import datetime
import pymysql as MySQLdb
import datetime
from dateutil.relativedelta import *

from RunTools import *
from FileTools import *
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir, get_tmpdir, get_existing_check_sums, write_meta_xml_main_processing
from libs.checks import runs_already_submitted
from libs.runs import get_run_status, clean_run, submit_run
from libs.dbtools import max_queue_id 
from libs.config import get_dataset_id_by_run
from libs.utils import DBChecksumCache

##-----------------------------------------------------------------
## setup DB
##-----------------------------------------------------------------

import SQLClient_dbs4 as dbs4

def main(params, logger, DryRun):
    END_RUN = params.ENDRUN
    START_RUN = params.STARTRUN
    Resubmission = params.RESUBMISSION

    dbs4_ = dbs4.MySQL()

    AllRuns = []

    MARKED_RUNS = []
    if (END_RUN) and (START_RUN) and END_RUN >= START_RUN:
        MARKED_RUNS = range(START_RUN,END_RUN+1)

    AllRuns.extend(MARKED_RUNS)

    if not len(AllRuns):
        logger.warning("No new runs to submit or old to update, check start:%s and end:%s run arguments"%(START_RUN,END_RUN))
        exit(0)

    if Resubmission and not args.out:
        if not runs_already_submitted(dbs4_, START_RUN, END_RUN, logger, DryRun):
            logger.critical('At least one run has not been submitted before. Do not use the resubmission flag to submit runs for the first time.')
            logger.critical('Exit')
            exit(1)
        elif not DryRun:
            dbs4_.execute("""UPDATE i3filter.grl_snapshot_info_pass2\
                                 SET submitted=0, \
                                     validated=0 \
                                 WHERE run_id BETWEEN %s AND %s AND (good_i3=1 OR good_it=1)"""%(START_RUN, END_RUN))

    checksumcache = DBChecksumCache(logger, DryRun)

    for Run in AllRuns:
        dataset_id = params.DATASETID

        if params.DATASETID is None:
            dataset_id = get_dataset_id_by_run(Run)
            if dataset_id < 0:
                logger.error("Could not get dataset id for run %s from config file. %s was returned. Skip this run." % (Run, dataset_id))
                continue
            else:
                logger.info("Dataset id of run %s was determined to %s" % (Run, dataset_id))

        logger.info("************** Attempting to (Re)submit %s"%(Run))

        if not args.out:
            GRLInfo = dbs4_.fetchall("""select g.*,r.tStart, r.tStop, r.FilesComplete from i3filter.grl_snapshot_info_pass2 g
                                    join i3filter.run_info_summary_pass2 r on r.run_id=g.run_id
                                    where g.run_id=%s and not submitted"""%(Run),UseDict=True)
        else:
            GRLInfo = dbs4_.fetchall("""select g.*,r.tStart, r.tStop, r.FilesComplete from i3filter.grl_snapshot_info_pass2 g
                                    join i3filter.run_info_summary_pass2 r on r.run_id=g.run_id
                                    where g.run_id=%s"""%(Run),UseDict=True)

        if not len(GRLInfo):
            logger.info("Run %s already submitted or no information for new submission "%Run)
            continue
        
        for g in GRLInfo:
            status = get_run_status(g)
           
            clean_run(dbs4_, dataset_id, Run,params.CLEANDW, g, logger, DryRun)
        
            QId = max_queue_id(dbs4_, dataset_id)
           
            if QId is None:
                QId = 0
 
            submit_run(dbs4_, g, status, dataset_id, QId, checksumcache, DryRun, logger, use_std_gcds = args.USE_STD_GCDS, gcd = args.gcd, input = args.input, out = args.out)
        
            if not args.NOMETADATA and (g['good_i3'] or g['good_it']):
                meta_file_dest = ''
                if DryRun:
                    meta_file_dest = get_tmpdir()
                else:
                    sDay = g['tStart']      # run start date
                    sY = sDay.year
                    sM = str(sDay.month).zfill(2)
                    sD = str(sDay.day).zfill(2)
    
                    meta_file_dest = "/data/exp/IceCube/%s/filtered/level2pass2/%s%s/Run00%s_%s" % (sY, sM, sD, g['run_id'], g['production_version'])

                    if args.out:
                        meta_file_dest = args.out.format(year = sY, month = sM, day = sD, run_id = g['run_id'], production_version = g['production_version'], dataset_id = dataset_id, snapshot_id = g['snapshot_id'])

                write_meta_xml_main_processing(dest_folder = meta_file_dest,
                                               dataset_id = dataset_id,
                                               run_id = g['run_id'],
                                               level = 'L2',
                                               run_start_time = g['tStart'],
                                               run_end_time = g['tStop'],
                                               logger = logger)
            else:
                logger.info("No meta data files will be written")
   
            if not DryRun and not args.out: 
                dbs4_.execute("""update i3filter.grl_snapshot_info_pass2\
                                 set submitted=1 \
                                 where run_id=%s and production_version=%s"""%\
                                 (g['run_id'],g['production_version']))

                

                logger.info("**************")

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)

    parser.add_argument("--datasetid", type=int, default=None,
                                      dest="DATASETID", help="The dataset id. The default value is `None`. In this case it gets the dataset id from the config file.")


    parser.add_argument("-s", "--startrun", type=int, required = True,
                                      dest="STARTRUN", help="start submission from this run")


    parser.add_argument("-e", "--endrun", type=int, required = False,
                                      dest="ENDRUN", help="end submission at this run")

    parser.add_argument("--input", type = str, required = False, default = None, help="Specify input path where the input files can be found. Use {year}, {month}, {day}, {run_id}, *, and [0-9] if needed. E.g. /data/exp/IceCube/{year}/filtered/PFFilt/{month}{day}/")

    parser.add_argument("--gcd", type = str, required = False, default = None, help="Specify the GCD path where the input files can be found. Use {year}, {month}, {day}, {run_id} if needed. E.g. /data/exp/IceCube/{year}/filtered/PFFilt/{month}{day}/")

    parser.add_argument("--out", type = str, required = False, default = None, help="Specify output path where the output files should be written. Use {year}, {month}, {day}, {run_id}, *, and [0-9] if needed. E.g. /data/exp/IceCube/{year}/filtered/PFFilt/{month}{day}/")

    parser.add_argument("-c", "--cleandatawarehouse", action="store_true", default=False,
              dest="CLEANDW", help="Clean output files in datawarehouse as part of (re)submission process.")


    parser.add_argument("-r", "--resubmission", action="store_true", default=False,
              dest="RESUBMISSION", help="Resubmit the runs. Note that all runs are resubmitted. If a run would be submitted for the very first time, an error is thrown.")

    parser.add_argument("-o", "--outputlog", default='',
                                      dest="OUTPUTLOG", help="Submission log file, default is time-stamped file name with log folder as location.")

    parser.add_argument("--nometadata", action="store_true", default=False,
              dest="NOMETADATA", help="Don't write meta data files")

    parser.add_argument("--use-std-GCDs", action="store_true", default=False,
              dest="USE_STD_GCDS", help="Don't use pass2 GCD files. Use pass1 GCD files. This flag should be set for season 2015, 2016... because those GCD files already have the SPE correction")

    args = parser.parse_args()

    LOGFILE=os.path.join(get_logdir(sublogpath = 'MainProcessing'), 'RunSubmission_')

    if args.ENDRUN is None:
        args.ENDRUN = args.STARTRUN

    if len(args.OUTPUTLOG):
        LOGFILE = args.OUTPUTLOG

    logger = get_logger(args.loglevel, LOGFILE)

    if args.DATASETID is None:
        logger.info("No dataset id is specified. The config file will be checked for each run to which dataset it belongs.")

    main(args, logger, args.dryrun)
