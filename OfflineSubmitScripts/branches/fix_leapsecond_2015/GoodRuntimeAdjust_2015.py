#!/usr/bin/env python
"""
Find and adjust start and end times of files concerning the good run times 
in live db.
"""


import os, sys
import glob
import datetime

from RunTools import RunTools
#from FileTools import *

import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

from icecube import icetray,dataclasses

from libs.files import get_logdir,GetSubRunStartStop,GetGoodSubruns,TrimFile,RemoveBadSubRuns
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.times import ComputeTenthOfNanosec

dbs4_ = dbs4.MySQL()
dbs2_ = dbs2.MySQL()
m_live = live.MySQL()
    
TOLERANCE = 5e4 #50 mu sec -> 5 events 

def main(RunNum,ProductionVersion,logger,dryrun=False):
    # get the run details
    RunInfo = dbs4_.fetchall("""SELECT * FROM i3filter.grl_snapshot_info g
                             join i3filter.run_info_summary r on r.run_id=g.run_id
                             where g.run_id=%s and production_version=%s"""%\
                            (RunNum,ProductionVersion),UseDict=True)
    
    if not len(RunInfo):
        logger.warning("no DB records for %s ..... exiting"%RunNum)
        exit(0) 
    
    RunInfo = RunInfo[0]
    R = RunTools(RunNum)
    OutFiles = R.GetRunFiles(RunInfo['tStart'],'L')
    if not len(OutFiles):
        logger.warning("No output L2 files for run %s, no files to adjust"%RunNum)
        exit(0) 
    
    ProdVersion = "%s_%s"%(str(RunInfo['run_id']),str(RunInfo['production_version']))
    
    GoodStart = dataclasses.I3Time(RunInfo['good_tstart'].year,ComputeTenthOfNanosec(RunInfo['good_tstart'],RunInfo['good_tstart_frac'])) 
    GoodEnd = dataclasses.I3Time(RunInfo['good_tstop'].year,ComputeTenthOfNanosec(RunInfo['good_tstop'],RunInfo['good_tstop_frac'])) 
    firstGood, lastGood, L2Files = GetGoodSubruns(OutFiles,GoodStart,GoodEnd,ProdVersion)
    logger.debug("""Database says GoodStart %s and GoodEnd %s""" %(GoodStart,GoodEnd))
    for file in [firstGood,lastGood]:
        start,stop = GetSubRunStartStop(file)
        logger.debug("""File %s starts at %s and stops at %s""" %(file,start,stop))
 
    # This moves subruns which are outside the goodruntime
    # to a BadNotWithinGoodRunRange subfolder
    RemoveBadSubRuns(L2Files,firstGood,lastGood,CleanDB=True,logger=logger,dryrun=dryrun)

    firstGoodStart, firstGoodStop = GetSubRunStartStop(firstGood,logger)
    lastGoodStart, lastGoodStop = GetSubRunStartStop(lastGood,logger)
    # Check if firstGood has to be trimmed
    if firstGoodStart < GoodStart:
        TrimFile(firstGood,GoodStart,GoodEnd,dryrun=dryrun,logger=logger)

    # Check if lastGood has to be trimmed
    if lastGoodStop > GoodEnd: 
        TrimFile(lastGood,GoodStart,GoodEnd,dryrun=dryrun,logger=logger)

    if (abs(firstGoodStart - GoodStart)) > TOLERANCE:
        logger.warning( "Discrepancy larger than %4.2e of Run start %s and grl table %s" %(TOLERANCE,firstGoodStart.__str__(), GoodStart.__str__()))

    if (abs(lastGoodStop - GoodEnd)) > TOLERANCE:
        logger.warning( "Discrepancy larger than %4.2e of Run start %s and grl table %s" %(TOLERANCE,lastGoodStop.__str__(), GoodEnd.__str__()))


if __name__ == '__main__':


    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument("run_id",type=int,help="RunId to process")
    parser.add_argument("production_version", type=int,help="Production version")
    args = parser.parse_args()
    logfile =os.path.join(get_logdir(sublogpath= "PostProcessing"),"GoodRunTime_adjust_")     
    logger = get_logger(args.loglevel,logfile)
    main(args.run_id,args.production_version,logger,dryrun=args.dryrun)    
