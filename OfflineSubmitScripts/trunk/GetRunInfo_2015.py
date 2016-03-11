#!/usr/bin/env python

"""
Gather information about recent runs from live database and migrate it to
the prodcution databases (i3filter on dbs4). Modifies tables grl_snapshot_info
and run_info_summary on dbs4
"""

import os
import re
import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

from sys import exit
from libs.logger import get_logger
from libs.argparser import get_defaultparser
import libs.checks
from RunTools import RunTools

dbs4_ = dbs4.MySQL()
m_live = live.MySQL()
dbs2_ = dbs2.MySQL()
    
def main(logger,dryrun=False):
    # Get the current production version and snapshot info 
    # from the production database dbs4
    CurrentInfo = dbs4_.fetchall("""select max(snapshot_id) as maxSnapshotID,
                                    max(production_version) as maxProductionV,
                                    max(ss_ref) as max_ss_ref
                                    from i3filter.grl_snapshot_info""",UseDict=True)
    
    CurrentMaxSnapshot = int(CurrentInfo[0]['maxSnapshotID'])
    CurrentProductionVersion = int(CurrentInfo[0]['maxProductionV'])
    ss_ref = int(CurrentInfo[0]['max_ss_ref']) + 1
    logger.debug("Got current max production_version %i and ss_ref %i from grl_snapshot_info table" %(CurrentMaxSnapshot,CurrentProductionVersion)) 
    
    IC86_5_FirstRun = "126378"  # 
    IC86_5_LastRun = "999999"   # change this when IC85_2015 season ends
    # including IC86_2015_24hr test runs taken during the IC86_2014 season
    #IC86_2015_24hr_TestRuns = (126289,126290,126291)

    # get the newest data from the live db      
    tmp_i3_ = m_live.fetchall(""" SELECT r.runNumber,r.tStart,r.tStop,
                                 r.tStart_frac,r.tStop_frac,r.nEvents,r.rateHz,
                                 l.snapshot_id,l.good_i3,l.good_it,l.reason_i3,l.reason_it,
                                 l.good_tstart, l.good_tstart_frac, l.good_tstop,l.good_tstop_frac
                                 FROM live.livedata_snapshotrun l
                                 join live.livedata_run r on l.run_id=r.id
                                 where (r.runNumber>=%s or r.runNumber in (126289,126290,126291))
                                 and r.runNumber<=%s
                                 order by l.snapshot_id"""%(IC86_5_FirstRun,IC86_5_LastRun),UseDict=True)
    
    if not len(tmp_i3_):
        logger.info("no results from i3Live DB for runs>=%s, snapshot_id>%s. no DB info. updated,exiting"%(IC86_5_FirstRun,CurrentMaxSnapshot))
        exit(0)
        
    # dict structure of live db data ensures only latest entry for every run is considered
    RunInfo_ = {}
    for r_ in tmp_i3_:
        for k in r_.keys():
            if r_[k] is None : r_[k]="NULL"
        RunInfo_[r_['runNumber']] = r_
    
    RunNums_ = RunInfo_.keys()
    RunNums_.sort()
    
    Run_SSId_Str_ = ",".join(["'"+str(r)+"_"+str(RunInfo_[r]['snapshot_id'])+"'" for r in RunNums_])
    RunStr_ = ",".join([str(r) for r in RunNums_])

    # get all previous runs from dbs4 and check if entries in live are different
    tmpRecords_ = dbs4_.fetchall("""select run_id from i3filter.run_info_summary
                                    order by run_id""",UseDict=True)
    tRecords_ = [t_['run_id'] for t_ in tmpRecords_]
    NewRecords_ = list(set(RunNums_).difference(set(tRecords_)))
    NewRecords_.sort()
    
    oRecords_ = dbs4_.fetchall("""select run_id from i3filter.grl_snapshot_info
                             where concat(run_id,"_",snapshot_id) in (%s)
                             order by run_id"""%Run_SSId_Str_,UseDict=True)
    OldRecords_ = [o_['run_id'] for o_ in oRecords_]
    
    cRecords_ = dbs4_.fetchall("""select s.run_id
                             from i3filter.grl_snapshot_info r
                             join i3filter.run_info_summary s on s.run_id=r.run_id
                             where concat(r.run_id,"_",r.snapshot_id) not in (%s)
                             and s.run_id in (%s)
                             order by s.run_id"""%(Run_SSId_Str_,RunStr_),UseDict=True)
    
    if len(cRecords_):
        logger.info("""The following records have changed and will result in  an update to
                 the ProductionVersion %s""" %cRecords.__repr__())
        continueProcessing = raw_input("Continue porcessing with updates (Y/N) : " )
    
        if continueProcessing.upper() != "Y":
            logger.info("halting processig due to user intervention ...")
            exit (0)
    
    ChangedRecords_ = [c_['run_id'] for c_ in cRecords_]
    if len(cRecords_):
        CurrentProductionVersion +=1
    
    if not len(NewRecords_) and not len(ChangedRecords_):
        logger.info("no records to be inserted/updated .. exiting")
        exit(0)
    
    for r in RunNums_:
        if r in OldRecords_ : continue
        if r in NewRecords_:
            logger.info("entering new records for run = %s"%r)
            R = RunTools(r,logger=logger)
            RunTimes = R.GetRunTimes()
            InFiles = R.GetRunFiles(RunTimes['tStart'],'P')
            CheckFiles = R.FilesComplete(InFiles,RunTimes)
   
            #  fill new runs from live in run_info_summary 
            if not dryrun: dbs4_.execute( """insert into i3filter.run_info_summary
                        (run_id,tStart,tStop,tStart_frac,tStop_frac,nEvents,rateHz,FilesComplete)
                        values(%u,"%s","%s","%s","%s",%s,%s,%u) """ \
                        %(r,RunInfo_[r]['tStart'],RunInfo_[r]['tStop'],
                        RunInfo_[r]['tStart_frac'],RunInfo_[r]['tStop_frac'],
                        RunInfo_[r]['nEvents'],RunInfo_[r]['rateHz'],CheckFiles))
        
        reason_i3 = ""
        reason_i3 = re.sub(r'[",]','',",".join(RunInfo_[r]['reason_i3'][1:-1].split(",")))
        reason_it = "" 
        reason_it = re.sub(r'[",]','',",".join(RunInfo_[r]['reason_it'][1:-1].split(",")))
    
        UpdateComment = ''    
        if r in ChangedRecords_:
            logger.info("updating records for run = %s"%r)
            UpdateComment = 'Updated in snapshot'
    
        goodStart = RunInfo_[r]['tStart']
        if RunInfo_[r]['good_tstart'] != "NULL" : goodStart = RunInfo_[r]['good_tstart']
        goodStart_frac = RunInfo_[r]['tStart_frac']
        if RunInfo_[r]['good_tstart_frac'] != "NULL" : goodStart_frac = RunInfo_[r]['good_tstart_frac']
        
        goodStop = RunInfo_[r]['tStop']
        if RunInfo_[r]['good_tstop'] != "NULL"  : goodStop = RunInfo_[r]['good_tstop']
        goodStop_frac = RunInfo_[r]['tStop_frac']
        if RunInfo_[r]['good_tstop_frac'] != "NULL"  : goodStop_frac = RunInfo_[r]['good_tstop_frac']
        # Check PFFilt files if there are empty and/or have no reading permission
        fileChkRlt = libs.checks.pffilt_size_and_permission(r, RunInfo_[r]['tStart'].year, RunInfo_[r]['tStart'].month, RunInfo_[r]['tStart'].day, logger, False)
        if len(fileChkRlt['empty']) + len(fileChkRlt['permission']) + len(fileChkRlt['emptyAndPermission']) > 0:
            logger.warning("Run %s has issues with PFFilt files"%r)
            warnstring = '  Empty files w/o reading permission:\n'
    	for file in fileChkRlt['emptyAndPermission']:
    	    warnstring += '    ' + file + '\n'
    
            warnstring += '  Empty files w/ reading permission:\n' 
            for file in fileChkRlt['empty']:  
                warnstring += '    ' + file + '\n'
    
            warnstring += '  Not empty files w/o reading permission:'
            for file in fileChkRlt['permission']:
                warnstring += '    ' + file + '\n'
    
        # insert new runs from live in grl_snapshot_info
        if not dryrun: dbs4_.execute( """insert into i3filter.grl_snapshot_info
                            (ss_ref,run_id,snapshot_id,good_i3,good_it,reason_i3,reason_it,
                            production_version,submitted,comments,good_tstart,good_tstart_frac,
                            good_tstop,good_tstop_frac)
                            values(%u,%u,%u,%u,%u,"%s","%s",%u,%u,"%s","%s",%s,"%s",%s)
                            """%(ss_ref,r,RunInfo_[r]['snapshot_id'],RunInfo_[r]['good_i3'],RunInfo_[r]['good_it'],
                            reason_i3,reason_it,CurrentProductionVersion,0,UpdateComment,
                            goodStart,goodStart_frac,goodStop,goodStop_frac))
        
        ss_ref+=1
    return 0


if __name__ == "__main__":

    parser = get_defaultparser(__doc__,dryrun=True)
    args = parser.parse_args()
    LOGFILE=os.path.join(os.path.split(__file__)[0],"logs/PreProcessing/GetRunInfo_")     
    logger = get_logger(args.loglevel,LOGFILE)
    main(logger,dryrun=args.dryrun)    


