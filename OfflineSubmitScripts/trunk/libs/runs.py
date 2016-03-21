
def submit_run(dbs4_, g, status, DatasetId, QueueId, ExistingChkSums, dryrun, logger):
    """
    Submits the run. It makes all the inserts to the database.

    Args:
        dbs4_ (SQLClient_dbs4): The SQL client for dbs4.
        g (dict): Dictionary of the result of a database query from `grl_snapshot_info`.
        status (str): Status of the run (see runs.get_run_status())
        DatasetId (int): The dataset id
        QueueId (int): The queue id
        ExistingChkSums (dict): Dictionary of existing check sums (see files.get_existing_check_sums())
        dryrun (bool): Is it a dry run?
        logger (logging.Logger): The logger
    """

    logger.info("""Submitting Run = %s , Current Status = %s"""%(g['run_id'],status))
    InFiles = []
    
    sDay = g['tStart']      # run start date
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)
    
    R = RunTools(g['run_id'], logger)
    InFiles = R.GetRunFiles(g['tStart'],'P')        
    
    MainOutputDir = OutputDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/"%(sY,sM,sD)
    if not os.path.exists(MainOutputDir) and not dryrun:
        os.mkdir(MainOutputDir)
    
    OutputDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/Run00%s_%s"%(sY,sM,sD,g['run_id'],g['production_version'])
    if not os.path.exists(OutputDir) and not dryrun:
        os.mkdir(OutputDir)
    
    GCDFileName = []
    GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/VerifiedGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
    
    if not len(GCDFileName):
        GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2/AllGCD/*Run00%s*%s_%s*"%(sY,g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
    
    if len(GCDFileName):
        GCDFileName = GCDFileName[0]
        GCDFileChkSum = str(FileTools(GCDFileName).md5sum())
        
        lnGCDFile = os.path.join(OutputDir,os.path.basename(GCDFileName))
        lnCmd = "ln -sf %s %s"%(GCDFileName,lnGCDFile)
        os.system(lnCmd)
    else:
        GCDFileName = ""
    
    if not len(InFiles):
        logger.info("No PFFilt will be submitted for run %s\n"%g['run_id'])
    
        QueueId+=1
    
        if not dryrun:
            dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
            dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,-1,str(sDay.date())))
    else:
        logger.info("Attempting to submit %s PFFilt Files for run %s"%(str(len(InFiles)),g['run_id']))
    
        for InFile in InFiles:
            CountSubRun = int(InFile[len(InFile)-16:-8])
            
            QueueId+=1
    
            if not dryrun:
                dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
    
            if os.path.isfile(GCDFileName) and not dryrun:
                dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DatasetId,QueueId,os.path.basename(GCDFileName),"file:"+os.path.dirname(GCDFileName)+"/",GCDFileChkSum,str(os.path.getsize(GCDFileName))))
    
            if InFile in ExistingChkSums:
                InFileChkSum = str(ExistingChkSums[InFile])
            else:
                InFileChkSum = str(FileTools(InFile).md5sum())
    
            if not dryrun:
                dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DatasetId,QueueId,os.path.basename(InFile),"file:"+os.path.dirname(InFile)+"/",InFileChkSum,str(os.path.getsize(InFile))))
    
                dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,CountSubRun,str(sDay.date())))

def clean_run(dbs4_,DatasetId,Run,CLEAN_DW,g, logger, dryrun):
    """
    Deletes all data in the database and, if requested, also in the datawarehouse.

    In particular, it cleans the database: `job`, `urlpath`, `run`.
    If the `CLEAN_DW` flag is set, it deletes existing files in the data warehouse.

    Args:
        dbs4_ (SQLClient_dbs4): The SQL client for dbs4.
        DatasetId (int): The dataset id
        Run (int): The run id
        CLEAN_DW (bool): If it is set to `True`, existing files in the data warehouse will be deleted.
        logger (logging.logger): The logger
        dryrun (bool): If it is `True`, no changes in the file system and the database are made.
    """
    tmp  = dbs4_.fetchall(""" select j.queue_id from i3filter.job j
                          join i3filter.run r on j.queue_id=r.queue_id
                          join i3filter.grl_snapshot_info g on r.run_id=g.run_id
                          where r.dataset_id=%s and j.dataset_id=%s
                          and r.run_id=%s and g.production_version=%s"""\
                          %(DatasetId,DatasetId,Run,g['production_version']) )
    
    if len(tmp):
        CleanListStr=""
        for t in tmp:
            CleanListStr+=(str(t[0])+",")
        CleanListStr = CleanListStr[:-1]
    
        #optional: also delete exisitng output files in data warehouse
        if CLEAN_DW and not dryrun:
            logger.info('Clean data warehouse')
            # clean only output files, exclude INPUT = {PFFilt, GCD} files
            tmp = dbs4_.fetchall(""" SELECT path,name FROM i3filter.urlpath
                                     where dataset_id=%s and queue_id in (%s) and type!="INPUT"
                                                     """%(DatasetId,CleanListStr))
    
            
            if len(tmp):
                for t in tmp:
                    filename = t[0][5:]+"/"+t[1]
                    if os.path.isfile(filename):
                        logger.info("deleting ", filename)
                        if not dryrun: os.system("rm -f %s"%filename)
    
    
                     #end: optional delete files in data warehouse
    
        if not dryrun:
            dbs4_.execute("""delete from i3filter.job where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))
    
            dbs4_.execute("""delete from i3filter.urlpath where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))
    
            dbs4_.execute("""delete from i3filter.run where dataset_id=%s and queue_id in (%s)"""%(DatasetId,CleanListStr))

def get_run_status(GRLInfo):
    """
    Returns the run status using information from the `grl_snapshot_info` table.

    Args:
        GRLInfo (dict): One dataset from `grl_snapshot_info`.

    Returns:
        str: The run status, see https://wiki.icecube.wisc.edu/index.php/IceProd_1.1.x#Job_Management
    """
    reason = GRLInfo['reason_i3'] + GRLInfo['reason_it']
    
    if (GRLInfo['good_i3'] or  GRLInfo['good_it']) and GRLInfo['GCDCheck']\
       and GRLInfo['BadDOMsCheck'] and GRLInfo['FilesComplete']:
        status = "WAITING"
    
    elif 'failed' in reason or 'spoiled' in reason:
        status="FailedRun"
    elif 'short' in reason:
        status='IDLEShortRun'
    elif 'test' in reason:
        status='IDLETestRun'
    elif 'lid' in reason:
        status = 'IDLELid'
            
    elif not GRLInfo['GCDCheck']:
        status = 'IDLENoGCD'

    elif not GRLInfo['BadDOMsCheck']:
        status = 'IDLEBDList'
        
    elif not GRLInfo['FilesComplete']:
        status = 'IDLEIncompleteFiles'
        
    else:
        status = "IDLE"

    return status
