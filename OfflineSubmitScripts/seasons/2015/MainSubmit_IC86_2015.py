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
from libs.files import get_logdir, get_existing_check_sums
from libs.checks import runs_already_submitted
from libs.runs import get_run_status, clean_run, submit_run
from libs.dbtools import max_queue_id 

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

    if Resubmission:
        if not runs_already_submitted(dbs4_, START_RUN, END_RUN, logger):
            logger.critical('At least one run has not been submitted before. Do not use the resubmission flag to submit runs for the first time.')
            logger.critical('Exit')
            exit(1)
        elif not DryRun:
            dbs4_.execute("""UPDATE i3filter.grl_snapshot_info\
                                 SET submitted=0 \
                                 WHERE run_id BETWEEN %s AND %s AND (good_i3=1 OR good_it=1)"""%(START_RUN, END_RUN))

    ExistingChkSums = get_existing_check_sums(logger)

    for Run in AllRuns:
        if DryRun:
            logger.info(Run)
        else:
            logger.info("************** Attempting to (Re)submit %s"%(Run))

            GRLInfo = dbs4_.fetchall("""select g.*,r.tStart,r.FilesComplete from i3filter.grl_snapshot_info g
                                    join i3filter.run_info_summary r on r.run_id=g.run_id
                                    where g.run_id=%s and not submitted"""%(Run),UseDict=True)
        
            if not len(GRLInfo):
                logger.info("Run %s already submitted or no information for new submission "%Run)
                continue
            
            for g in GRLInfo:
                status = get_run_status(g)
               
                clean_run(dbs4_,params.DATASETID,Run,params.CLEANDW,g)
            
                QId = max_queue_id(dbs4_,params.DATASETID)
                
                submit_run(dbs4_,g,status,params.DATASETID,QId,ExistingChkSums,DryRun,logger)
               
                if not DryRun: 
                    dbs4_.execute("""update i3filter.grl_snapshot_info\
                                     set submitted=1 \
                                     where run_id=%s and production_version=%s"""%\
                                     (g['run_id'],g['production_version']))

                logger.write("**************")

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)

    parser.add_argument("--datasetid", type=int, default=1883,
                                      dest="DATASETID", help="The dataset id. The default value is 1883.")


    parser.add_argument("-s", "--startrun", type=int, required = True,
                                      dest="STARTRUN", help="start submission from this run")


    parser.add_argument("-e", "--endrun", type=int, required = True,
                                      dest="ENDRUN", help="end submission at this run")


    parser.add_argument("-c", "--cleandatawarehouse", action="store_true", default=False,
              dest="CLEANDW", help="Clean output files in datawarehouse as part of (re)submission process.")


    parser.add_argument("-r", "--resubmission", action="store_true", default=False,
              dest="RESUBMISSION", help="Resubmit the runs. Note that all runs are resubmitted. If a run would be submitted for the very first time, an error is thrown.")

    parser.add_argument("-o", "--outputlog", default='',
                                      dest="OUTPUTLOG", help="Submission log file, default is time-stamped file name with log folder as location.")


    args = parser.parse_args()

    LOGFILE=os.path.join(get_logdir(sublogpath = 'MainProcessing'), 'RunSubmission_')

    if len(args.OUTPUTLOG):
        LOGFILE = args.OUTPUTLOG

    logger = get_logger(args.loglevel, LOGFILE)

    main(args, logger, args.dryrun)
