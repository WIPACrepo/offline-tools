
from RunTools import RunTools
from FileTools import FileTools
import os
import glob
from logger import DummyLogger

def submit_run(dbs4_, g, status, DatasetId, QueueId, checksumcache, dryrun, logger, use_std_gcds = False, gcd = None, input = None, out = None, add = False):
    """
    Submits the run. It makes all the inserts to the database.

    Args:
        dbs4_ (SQLClient_dbs4): The SQL client for dbs4.
        g (dict): Dictionary of the result of a database query from `grl_snapshot_info_pass2`.
        status (str): Status of the run (see runs.get_run_status())
        DatasetId (int): The dataset id
        QueueId (int): The queue id
        dryrun (bool): Is it a dry run?
        logger (logging.Logger): The logger
    """

    from config import get_season_by_run, get_config
    from files import remove_path_prefix

    path_prefix = 'gsiftp://gridftp.icecube.wisc.edu'

    logger.info("""Submitting Run = %s , Current Status = %s"""%(g['run_id'],status))
    InFiles = []
    
    sDay = g['tStart']      # run start date
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)
    
    format_path = lambda p: p.format(year = sY, month = sM, day = sD, run_id = g['run_id'], production_version = g['production_version'], dataset_id = DatasetId, snapshot_id = g['snapshot_id'])

    R = RunTools(g['run_id'], logger, passNumber = 2)

    logger.debug('Get PFFilt files')

    season = get_season_by_run(g['run_id'])

    logger.debug('Run %s is of season %s' % (g['run_id'], season))

    InFiles = R.GetRunFiles(g['tStart'], 'P', season = season)
 
    excluded_files = [
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000019.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000020.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000021.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000022.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000023.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000024.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000025.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000026.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000027.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000028.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000029.tar.gz',
        'PFDST_TestData_Unfiltered_Run00120516_Subrun00000000_0000030.tar.gz'
    ]

    InFiles = [f for f in InFiles if os.path.basename(f) not in excluded_files]

    if add:
        sql = '''SELECT 
    path, name
FROM
    i3filter.run r
        JOIN
    i3filter.urlpath u ON r.dataset_id = u.dataset_id
        AND r.queue_id = u.queue_id
WHERE
    r.dataset_id = {dataset_id} AND run_id = {run_id}
        AND `type` = 'INPUT'
        AND name NOT LIKE '%GCD%'
ORDER BY name'''.format(dataset_id = DatasetId, run_id = g['run_id'])

        submitted_files = dbs4_.fetchall(sql, UseDict = True)

        submitted_files = [os.path.join(remove_path_prefix(f['path']), f['name']) for f in submitted_files]

        InFiles = [f for f in InFiles if f not in submitted_files]

        logger.warning('The --add option has been activated. Only {} files will be submitted in addition to the already submitted ones.'.format(len(InFiles)))

        logger.debug('Already submitted files:')
        for f in submitted_files:
            logger.debug('  ' + f)
 
    if input:
        logger.debug("InFiles glob = %s" % format_path(input))
        InFiles = sorted(glob.glob(format_path(input)))

    # Filter .xml, .bad, .error files
    # .bad and .error files are used by Jim Bellinger to mark corrupted files. Sometimes the corrupted and the good files are available.
    for e in get_config().get('DEFAULT', 'IgnoreFilesWithExtension').split(','):
        InFiles = [f for f in InFiles if not f.endswith(e)]
 
    logger.debug("InFiles = %s" % InFiles)
 
    MainOutputDir = OutputDir = "/data/exp/IceCube/%s/filtered/level2pass2/%s%s/"%(sY,sM,sD)
    
    if out:
        MainOutputDir = OutputDir = format_path(out)

    logger.debug("MainOutputDir = %s" % MainOutputDir)

    if not os.path.exists(MainOutputDir) and not dryrun:
        os.makedirs(MainOutputDir)
    
    OutputDir = "/data/exp/IceCube/%s/filtered/level2pass2/%s%s/Run00%s_%s"%(sY,sM,sD,g['run_id'],g['production_version'])

    if out:
        OutputDir = format_path(out)

    logger.debug("OutputDir = %s" % OutputDir)

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

    if gcd:
        GCDFileName = list(reversed(sorted(glob.glob(format_path(gcd)))))

    if not len(GCDFileName) and not gcd:
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
   
            InFileChkSum = checksumcache.get_md5(InFile)
    
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

def get_validated_runs_L2(dataset_id, logger):
    """
    Returns a list of runs that are validated at L2.

    Args:
        dbs4 (SQLClient_dbs4): The SQL client for dbs4
        runs (list): List of runs

    Returns:
        list: List of validated runs within the given range
    """

    from databaseconnection import DatabaseConnection
    db = DatabaseConnection.get_connection('filter-db', logger)

    sql = 'SELECT run_id FROM i3filter.post_processing WHERE dataset_id = {} AND validated'.format(dataset_id)
    result = db.fetchall(sql, UseDict = True)
    return [int(r['run_id']) for r in result]

def get_validated_runs(dataset_id, use_dict = True, logger = DummyLogger()):
    """
    Returns all validated runs of the given dataset. In addition, it returns also the
    date of validation.

    Args:
        dataset_id (int): The dataset id
        use_dict(bool): If `True` (default), the db result is returned as list of dicts
        logger (logging.Logger): The logger. Default is the DummyLogger that just prints the messages.

    Returns:
        list: The SQL result
    """

    from databaseconnection import DatabaseConnection
    db = DatabaseConnection.get_connection('filter-db', logger)

    sql = "SELECT * FROM i3filter.post_processing WHERE dataset_id = %s AND validated = 1" % int(dataset_id)

    logger.debug("SQL: %s" % sql)

    return db.fetchall(sql, UseDict = use_dict)


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
