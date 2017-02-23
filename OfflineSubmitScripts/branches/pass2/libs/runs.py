
from RunTools import RunTools
from FileTools import FileTools
import os
import glob
from logger import DummyLogger

def submit_run(dbs4_, g, status, DatasetId, QueueId, ExistingChkSums, dryrun, logger, use_std_gcds = False):
    """
    Submits the run. It makes all the inserts to the database.

    Args:
        dbs4_ (SQLClient_dbs4): The SQL client for dbs4.
        g (dict): Dictionary of the result of a database query from `grl_snapshot_info_pass2`.
        status (str): Status of the run (see runs.get_run_status())
        DatasetId (int): The dataset id
        QueueId (int): The queue id
        ExistingChkSums (dict): Dictionary of existing check sums (see files.get_existing_check_sums())
        dryrun (bool): Is it a dry run?
        logger (logging.Logger): The logger
    """

    # Using grid or NPX?
    grid = dbs4_.fetchall("SELECT * FROM i3filter.grid_statistics WHERE dataset_id = %s;" % DatasetId, UseDict = True)

    logger.debug("DB result = %s" % grid)

    path_prefix = 'file:'
    for row in grid:
        if row['grid_id'] == 14:
            path_prefix = 'gsiftp://gridftp.icecube.wisc.edu'
            break

    logger.info("""Submitting Run = %s , Current Status = %s"""%(g['run_id'],status))
    InFiles = []
    
    sDay = g['tStart']      # run start date
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)
    
    R = RunTools(g['run_id'], logger, passNumber = 2)

    logger.debug('Get PFFilt files')

    InFiles = R.GetRunFiles(g['tStart'],'P')        
   
    logger.debug("InFiles = %s" % InFiles)
 
    MainOutputDir = OutputDir = "/data/exp/IceCube/%s/filtered/level2pass2/%s%s/"%(sY,sM,sD)
    if not os.path.exists(MainOutputDir) and not dryrun:
        os.mkdir(MainOutputDir)
    
    OutputDir = "/data/exp/IceCube/%s/filtered/level2pass2/%s%s/Run00%s_%s"%(sY,sM,sD,g['run_id'],g['production_version'])
    if not os.path.exists(OutputDir) and not dryrun:
        os.mkdir(OutputDir)
    
    logger.debug('Find GCD file')

    pass2GCD = 'pass2'

    if use_std_gcds:
        pass2GCD = ''

    GCDFileName = []

    if not use_std_gcds:
        GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2%s/VerifiedGCD/*Run00%s*%s_%s*"%(sY, pass2GCD, g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
    else:
        GCDFileName = list(reversed(sorted(glob.glob("/data/exp/IceCube/%s/filtered/level2%s/VerifiedGCD/*Run00%s*_%s*"%(sY, pass2GCD, g['run_id'],str(g['snapshot_id']))))))

    if not len(GCDFileName):
        if not use_std_gcds:
            GCDFileName = glob.glob("/data/exp/IceCube/%s/filtered/level2%s/AllGCD/*Run00%s*%s_%s*"%(sY, pass2GCD, g['run_id'],str(g['production_version']),str(g['snapshot_id'])))
        else:
            GCDFileName = list(reversed(sorted(glob.glob("/data/exp/IceCube/%s/filtered/level2%s/AllGCD/*Run00%s*_%s*"%(sY, pass2GCD, g['run_id'],str(g['snapshot_id']))))))
   
    logger.debug("GCDFileName = %s" % GCDFileName)
 
    if len(GCDFileName):
        logger.debug('Calculate MD5 sum and create symlink for GCD file')

        GCDFileName = GCDFileName[0]
        GCDFileChkSum = str(FileTools(GCDFileName, logger).md5sum())
        
        lnGCDFile = os.path.join(OutputDir,os.path.basename(GCDFileName))
        
        if not dryrun:
            lnCmd = "ln -sf %s %s"%(os.path.relpath(GCDFileName, os.path.dirname(lnGCDFile)), lnGCDFile)
            os.system(lnCmd)
    else:
        GCDFileName = ""
    
    if not len(InFiles):
        logger.info("No PFFilt will be submitted for run %s"%g['run_id'])
    
        QueueId+=1
    
        if not dryrun:
            dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
            dbs4_.execute("""insert into i3filter.run (run_id,dataset_id,queue_id,sub_run,date) values (%s,%s,%s,%s,"%s")"""%(g['run_id'],DatasetId,QueueId,-1,str(sDay.date())))
    else:
        logger.info("Attempting to submit %s PFFilt Files for run %s"%(str(len(InFiles)),g['run_id']))
    
        for InFile in InFiles:
            logger.debug("InFile = %s" % InFile)

            CountSubRun = int(InFile[len(InFile)-15:-7])
           
            logger.debug("CountSubRun = %s" % CountSubRun)
 
            QueueId+=1
    
            if not dryrun:
                dbs4_.execute("""insert into i3filter.job (dataset_id,queue_id,status) values (%s,%s,"%s")"""%(DatasetId,QueueId,status))
    
            if os.path.isfile(GCDFileName) and not dryrun:
                dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DatasetId,QueueId,os.path.basename(GCDFileName),path_prefix+os.path.dirname(GCDFileName)+"/",GCDFileChkSum,str(os.path.getsize(GCDFileName))))
    
            if InFile in ExistingChkSums:
                logger.debug("Use cached check sum for %s" % InFile)
                InFileChkSum = str(ExistingChkSums[InFile])
            else:
                logger.warning("No cached check sum for %s" % InFile)
                InFileChkSum = str(FileTools(InFile, logger).md5sum())
    
            if not dryrun:
                dbs4_.execute("""insert into i3filter.urlpath (dataset_id,queue_id,name,path,type,md5sum,size) values ("%s","%s","%s","%s","INPUT","%s","%s")"""% \
                             (DatasetId,QueueId,os.path.basename(InFile),path_prefix+os.path.dirname(InFile)+"/",InFileChkSum,str(os.path.getsize(InFile))))
    
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
                          join i3filter.grl_snapshot_info_pass2 g on r.run_id=g.run_id
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
    Returns the run status using information from the `grl_snapshot_info_pass2` table.

    Args:
        GRLInfo (dict): One dataset from `grl_snapshot_info_pass2`.

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

def get_validated_runs_L2(dbs4, range_start, range_end):
    """
    Returns a list of runs that are validated at L2.

    Args:
        dbs4 (SQLClient_dbs4): The SQL client for dbs4
        range_start (int): Run number start (included)
        range_end (int): Run number end (included)

    Returns:
        list: List of validated runs within the given range
    """

    sql = """SELECT 
                run_id
            FROM
                i3filter.grl_snapshot_info_pass2
            WHERE
                submitted AND validated
                    AND run_id BETWEEN %s AND %s""" % (range_start, range_end)

    result = dbs4.fetchall(sql, UseDict = True)
    return [r['run_id'] for r in result]

def get_validated_runs(dataset_id, dbs4, use_dict = True, logger = DummyLogger()):
    """
    Returns all validated runs of the given dataset. In addition, it returns also the
    date of validation.

    Args:
        dataset_id (int): The dataset id
        dbs4 (SQLClient_dbs4): The SQL client for dbs4
        use_dict(bool): If `True` (default), the db result is returned as list of dicts
        logger (logging.Logger): The logger. Default is the DummyLogger that just prints the messages.

    Returns:
        list: The SQL result
    """

    sql = "SELECT * FROM offline_postprocessing WHERE dataset_id = %s AND validated = 1" % int(dataset_id)

    logger.debug("SQL: %s" % sql)

    return dbs4.fetchall(sql, UseDict = use_dict)


def set_post_processing_state(run_id, dataset_id, validated, dbs4, dryrun, logger = DummyLogger()):
    """
    Sets the flag for the specified run and dataset in the database.

    Args:
        run_id (int): The run id
        dataset_id (int): The dataset id of the run
        validated (bool): `True`, if the run has been validated
        dbs4 (SQLClient_dbs4): The SQL client for dbs4
        dryrun (bool): If `True`, nothing will be written into the database
        logger (logging.Logger): The logger. Default is the DummyLogger that just prints the messages.
    """

    run_id = int(run_id)
    dataset_id = int(dataset_id)
    validated = int(validated)

    sql = """   INSERT INTO offline_postprocessing
                    (run_id, dataset_id, validated, date_of_validation)
                VALUES
                    (%s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    validated = %s,
                    date_of_validation = NOW()
                    """ % (run_id, dataset_id, validated, validated)

    logger.debug("SQL: %s" % sql)

    if not dryrun:
        dbs4.execute(sql)

def get_run_lifetime(run_id, logger):
    from databaseconnection import DatabaseConnection

    run_id = int(run_id)
    result = {
        'run_id': run_id,
        'sub_runs': None,
        'simple_livetime': None,
        'sub_run_livetime': None,
        'gaps': None,
        'gaps_time': None,
        'livetime': None
    }

    # Databases
    dbs4 = DatabaseConnection.get_connection('dbs4', logger)
    filter_db = DatabaseConnection.get_connection('filter-db', logger)

    # Load good start/stop times for cross check
    sql = "SELECT TIMESTAMPDIFF(SECOND, good_tstart, good_tstop) AS `livetime` FROM i3filter.grl_snapshot_info_pass2 WHERE run_id = %s" % run_id
    data = dbs4.fetchall(sql, UseDict = True)[0]
    result['simple_livetime'] = data['livetime']

    # Load livetime from gaps files/sub_run table
    sql = "SELECT SUM(livetime) AS `livetime`, COUNT(*) AS `sub_runs` FROM i3filter.sub_runs_pass2 WHERE run_id = %s" % run_id
    data = filter_db.fetchall(sql, UseDict = True)[0]
    result['sub_run_livetime'] = data['livetime']
    result['sub_runs'] = data['sub_runs']

    # Load total gaps lifetime
    sql = "SELECT COUNT(*) AS `gaps`, SUM(delta_time) AS `livetime` FROM i3filter.gaps_pass2 WHERE run_id = %s" % run_id
    data = filter_db.fetchall(sql, UseDict = True)[0]
    result['gaps'] = data['gaps']
    result['gaps_time'] = data['livetime']

    if result['gaps_time'] is None:
        result['gaps_time'] = 0
   
    if result['sub_run_livetime'] is None:
        logger.warning("Run %s has no information of sub run livetimes. Using good start/stop times." % run_id)

        if result['simple_livetime'] is None:
            logger.error("No livetime for run %s could be determined" % run_id)
            return None

        result['livetime'] = result['simple_livetime'] - result['gaps_time']
    else:
        result['livetime'] = result['sub_run_livetime'] - result['gaps_time']

    logger.debug(result)

    return result
