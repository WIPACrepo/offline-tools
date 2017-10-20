#!/usr/bin/env python
"""
Combines several checks wich have to be done after the
files are generated and updates the databases accordingly
"""


import os, sys
import subprocess as sub
import time
import datetime
import argparse
from dateutil.relativedelta import *

import libs.files
from libs.config import get_seasons_info, get_config, get_dataset_id_by_run, get_season_by_run
sys.path.append(get_config().get('DEFAULT', 'SQLClientPath'))
sys.path.append(get_config().get('DEFAULT', 'ProductionToolsPath'))
from RunTools import RunTools
from FileTools import *
from DbTools import *

from libs.files import get_tmpdir, get_logdir, tar_log_files, insert_gap_file_info_and_delete_files
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.utils import DBChecksumCache

from libs.checks import PrintVerboseDifference

import libs.process

from libs.databaseconnection import DatabaseConnection
from libs import dbtools

import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

m_live = live.MySQL()    
dbs4_ = dbs4.MySQL()   
dbs2_ = dbs2.MySQL()    

def remove_path_prefix(path):
    """
    Removes `file:` or `gsiftp://gridftp.icecube.wisc.edu` from path.
    """

    prefix = ['file:', 'gsiftp://gridftp.icecube.wisc.edu']

    for p in prefix:
        if path.startswith(p):
            return path[len(p):]

    return path

def get_in_files(run_id, start_date):
    return get_x_files(run_id, start_date, '/data/exp/IceCube/{year}/unbiased/PFRaw/{month:0>2}{day:0>2}/*PFRaw_*PhysicsFiltering_Run{run_id:0>8}_Subrun00000000_00000*.tar.gz')

def get_out_files(run_id, start_date, season):
    dst_folder = 'PFDST'
    if int(season) in (2015, 2016):
        dst_folder += 'noSPE'

    return get_x_files(run_id, start_date, '/data/exp/IceCube/{year}/unbiased/' + dst_folder + '/{month:0>2}{day:0>2}/PFDST_PhysicsFiltering_Run*{run_id:0>8}_Subrun00000000_00000*.i3.bz2')

def get_x_files(run_id, start_date, folder_pattern):
    from glob import glob

    def f(d, date, run_id):
        return d.format(year = date.year, month = date.month, day = date.day, run_id = run_id)

    next_day = start_date + relativedelta(days = 1)

    folder1 = f(folder_pattern, start_date, run_id)
    folder2 = f(folder_pattern, next_day, run_id)

    gcd = f(os.path.join(os.path.dirname(folder_pattern), '*{run_id}*GCD*'), start_date, run_id)

    files = glob(folder1)
    files.extend(glob(folder2))
    files.extend(glob(gcd))

    return files

def ICECUBE_GCDDIR(date, run_id):
    if get_season_by_run(run_id) != 2010:
        return "/data/exp/IceCube/%s/filtered/level2/VerifiedGCD" % str(date.year)
    else:
        return "/data/exp/IceCube/{year}/filtered/level2a/{month:0>2}{day:0>2}".format(year = date.year, month = date.month, day = date.day)

def CheckFiles(r, logger, dataset_id, season, run_id, dryrun, checksumcache, force):
    if not r['gcd_generated'] or not r['gcd_bad_doms_validated']:
        logger.info("gcd_generated or gcd_bad_doms_validated failed for run=%s, production_version%s" %(str(r['run_id']),str(r['production_version'])))
        return 1
    
    InFiles = get_in_files(run_id, r['tstart'])
    OutFiles = get_out_files(run_id, r['tstart'], season)

    # Check for "recovered-data_*" files. If such files are present, remove same file w/o prefix
    recovered_data_files = [f for f in InFiles if os.path.basename(f).startswith('recovered-data_')]

    for rf in recovered_data_files:
        orig_name = os.path.join(os.path.dirname(rf), os.path.basename(rf).split('recovered-data_')[1])
        if orig_name in InFiles:
            InFiles.remove(orig_name)
            logger.warning('Found a file with recovered data: {}'.format(rf))
            logger.warning('Also the original file has been found: {}'.format(orig_name))
            logger.warning('The original file will be ignored.')
  
    #logger.debug('OutFiles: %s' % OutFiles)
 
    # Remove all BadRuns from InFiles:
    bad_runs = dbtools.get_bad_sub_runs(dbs4 = dbs4_, dataset_id = dataset_id, run_id = r['run_id'], logger = logger)

    for bad_run in bad_runs:
        path = os.path.join(bad_run['path'], bad_run['name'])[5:]
        InFiles = filter(lambda e: e != path, InFiles)

    ProdVersion = "%s_%s/"%(str(r['run_id']),str(r['production_version']))
    
    Files2Check = []
    
    # check for multiple GCD files in out Dir, usually results from re-processing
    
    GCDName = [f for f in OutFiles if "GCD" in f]

    logger.debug('GCDName = %s' % GCDName)

    if len(GCDName)!=1:
        logger.warning("Either None or more than 1 GCD file in output dir for run=%s"%str(r['run_id']))

        if not force:
            return 1
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
    
    GCDName = GCDName[0]
    GCDName = os.path.join(ICECUBE_GCDDIR(r['tstart'], r['run_id']),os.path.basename(GCDName))
    Files2Check.append(GCDName)
    
    L2Files = [f for f in OutFiles if "GCD" not in f \
                   and "txt" not in f and "root" not in f\
                   and "EHE" not in f and "IT" not in f \
                   and "log" not in f]  
    L2Files.sort()
    
    logger.debug('InFiles: %s' % InFiles)
    logger.debug('OutFiles: %s' % L2Files)

    if len(InFiles) != len(L2Files):
        logger.warning("No. of Input and Output files don't match for run=%s, production_version=%s" %(str(r['run_id']),str(r['production_version'])))
        logger.warning('Input files ({0}), Output files ({1})'.format(len(InFiles), len(L2Files)))

        if not force:
            return 1
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')

    for p in InFiles:
        l = os.path.join(
            os.path.dirname(L2Files[0]),
            #'PFDST_' + os.path.basename(p).split('PFRaw_')[1].replace(".tar",".i3").replace('.gz', '.bz2')
            'PFDST_PhysicsFiltering_Run{run_id:0>8}_Subrun00000000_{sub_run:0>8}.i3.bz2'.format(run_id = r['run_id'], sub_run = int(p.split('.')[0].split('_00')[-1]))
        )
   
        # IC 79 configuration bug: accidently configured too many zeros before the run id 
        l2 = os.path.join(
            os.path.dirname(L2Files[0]),
            'PFDST_PhysicsFiltering_Run{run_id:0>10}_Subrun00000000_{sub_run:0>8}.i3.bz2'.format(run_id = r['run_id'], sub_run = int(p.split('.')[0].split('_00')[-1]))
        )
    
        if not os.path.isfile(l) and not os.path.isfile(l2):
            logger.warning("At least one output file %s does not exist for input file %s"%(l,p))

            if not force:
                return 1
            else:
                logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')

        Files2Check.append(p)
        Files2Check.append(l if os.path.isfile(l) else l2)
    
    Files2CheckS = """'""" + """','""".join(Files2Check) + """'"""
    
    sql = """SELECT distinct name, path
                                  FROM i3filter.urlpath u
                                 WHERE u.dataset_id = {dataset_id} AND (
                                 concat(substring(u.path,6),"/",u.name) IN ({files}) OR \
                                 concat(substring(u.path,6),u.name) IN ({files}) OR \
                                 concat(substring(u.path,34),u.name) IN ({files}) OR \
                                 concat(substring(u.path,34),"/",u.name) IN ({files}))""".format(dataset_id = dataset_id, files = Files2CheckS)
 
    logger.debug('SQL: {0}'.format(sql))
 
    FilesInDb = dbs4_.fetchall(sql, UseDict = True)
 
    logger.debug(FilesInDb)
 
    FilesInDb = list(set([os.path.join(remove_path_prefix(f['path']), f['name']) for f in FilesInDb]))

    logger.debug('Files = %s' % FilesInDb)

    if len(Files2Check) != len(FilesInDb):
        logger.warning("Some file records don't exist for run=%s, production_version=%s" %(str(r['run_id']),str(r['production_version'])))
        PrintVerboseDifference(Files2Check,FilesInDb,logger) 

        if not force:
            return 1
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
   
    # Check MD5 sums
    outputFileInfos = dbs4_.fetchall("""SELECT sub_run, name, path, size, md5sum
                                        FROM run r
                                        JOIN urlpath u 
                                            ON u.queue_id = r.queue_id 
                                            AND u.dataset_id = r.dataset_id 
                                        WHERE   r.dataset_id = %s 
                                            AND type = 'PERMANENT'
                                            AND transferstate <> 'DELETED' 
                                            AND run_id = %s""" % (dataset_id, r['run_id']),
                                    UseDict = True)

    for i, subrunInfo in enumerate(outputFileInfos):
        path = os.path.join(remove_path_prefix(subrunInfo['path']), subrunInfo['name'])

        if path in OutFiles:
            logger.info('(%s / %s)\tCheck checksums for %s' % (i, len(outputFileInfos), path))

            md5sum = FileTools(FileName = path, logger = logger).md5sum()

            # Check if checksum matches the checksum in DB
            if md5sum == subrunInfo['md5sum']:
                logger.debug("MD5 check sums match for %s" % path)
                checksumcache.set_md5(path, md5sum)
            else:
                logger.warning("MD5 check sum mismatch for %s" % path)
                return 1
        else:
            logger.warning("File %s is listed in the database as PERMANENT but doesn not exist" % path)

            if not force:
                return 1
            else:
                logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
 
    # all checks passed
    return 0

def main_run(r, logger, season, args, checksumcache):
    dryrun = args.dryrun
    dataset_id = args.dataset_id

    logger.info("======= Checking %s %s ==========="  %(str(r['run_id']),str(r['production_version'])))

    if DbTools(r['run_id'], dataset_id).AllOk():
        logger.warning( """Processing of Run=%s, production_version=%s
                 may not be complete ... skipping"""\
                %(r['run_id'],str(r['production_version'])))

        if not args.force_validation:
            return
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
     
    # check i/o files in data warehouse and Db
    logger.info("Checking Files in Data warehouse and database records ...")
    if CheckFiles(r, logger, dataset_id = dataset_id, season = season, run_id = r['run_id'], dryrun = dryrun, checksumcache = checksumcache, force = args.force_validation):
        logger.error("FilesCheck failed: for Run=%s, production_version=%s"\
        %(r['run_id'],str(r['production_version'])))

        if not args.force_validation:
            return
        else:
            logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
     
    logger.info("File checks  .... passed")

    sql = """   INSERT INTO post_processing
                    (run_id, dataset_id, validated, date_of_validation)
                VALUES
                    (%s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    validated = %s,
                    date_of_validation = NOW()
                    """ % (r['run_id'], args.dataset_id, 1, 1)

    logger.debug("SQL: %s" % sql)

    if not dryrun:
        filter_db = DatabaseConnection.get_connection('filter-db', logger)
        filter_db.execute(sql)

    sDay = r['tstart']
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)

    dst_folder = 'PFDST'
    if int(season) in (2015, 2016):
        dst_folder += 'noSPE'

    run_folder = "/data/exp/IceCube/%s/unbiased/%s/%s%s" % (sY, dst_folder, sM, sD)

    logger.debug('tar log files')

    tar_log_files(run_path = run_folder, dryrun = dryrun, logger = logger)

    logger.info("Checks passed")
    logger.info("======= End Checking %i %i ======== " %(r['run_id'],r['production_version'])) 
    return

def main(runinfo, logger, season, args):
    checksumcache = DBChecksumCache(logger, args.dryrun)
    fdb = DatabaseConnection.get_connection('filter-db', logger)
    sql = "SELECT * FROM i3filter.post_processing WHERE dataset_id = {0} AND validated".format(args.dataset_id)
    validated_runs = fdb.fetchall(sql, UseDict = True)
    validated_runs = [int(r['run_id']) for r in validated_runs]

    logger.debug('Validated runs: %s' % validated_runs)

    for run in runinfo:
        try:
            if int(run['run_id']) not in validated_runs:
                main_run(run, logger, season, args, checksumcache)
            else:
                logger.info('Skip run %s because it has already been validated' % run['run_id'])
        except Exception as e:
            logger.exception("Exception %s thrown for: Run=%s, production_version=%s" %(e.__repr__(),run['run_id'],str(run['production_version'])))
   
if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument('-r',nargs="?", help="run to postprocess",dest="run",type=int)
    parser.add_argument('--season', nargs="?", default = None, help="Validate a specific season. If not specified (and not -r) the season that has been set in the confog file will be chosen", type=int)
    parser.add_argument('--dataset-id', required = True, help="The dataset id", type=int)
    parser.add_argument("--force-validation", nargs='+', default = None, required = None, type = str, help = "DO ONLY USE THIS IF YOU KNOW THAT THE ERRORS ARE WRONG. Validates the run(s) despite there are errors. Makes an entry into filter-db.run_comments. If you use this flag, the argument will be used as comment. E.g. '--force-validation \"run times are OK\"'. This will lead to a comment like: \"Validated manually. run times are OK\"")
    args = parser.parse_args()

    if args.force_validation is not None:
        args.force_validation = ' '.join(args.force_validation)

    LOGFILE=os.path.join(get_logdir(), 'SDST_PostProcessing_')

    logger = get_logger(args.loglevel, LOGFILE)

    # Check if --cron option is enabled. If so, check if cron usage allowed by config
    lock = None

    season = args.season or get_config().getint('DEFAULT', 'Season')
    seasons_info = get_seasons_info()

    logger.debug('Seaosns info: %s' % seasons_info)

    next_test_runs = []
    last_run = 9999999
    if season + 1 in seasons_info:
        if seasons_info[season + 1]['first'] > 0:
            last_run = seasons_info[season + 1]['first']

        next_test_runs = seasons_info[season + 1]['test']

    logger.debug('Season: %s' % season)

    RunInfo = None

    fdb = DatabaseConnection.get_connection('filter-db', logger)

    if args.run is not None:
        RunInfo = fdb.fetchall("""SELECT * FROM i3filter.runs g
                                  WHERE (g.good_i3 OR g.good_it or g.run_id IN (%s)) AND g.run_id = %i
                                  ORDER BY g.run_id""" % (','.join([str(r) for r in seasons_info[season]['test']]), args.run), UseDict=True)
    else: 
        sql = """
SELECT 
    *
FROM
    i3filter.runs g
WHERE
    (g.run_id BETWEEN {first_run} AND {last_run}
        OR g.run_id IN ({test_runs},-1))
        AND g.run_id NOT IN ({next_test_runs},-1)
        AND (good_it OR good_i3
        OR g.run_id IN ({test_runs},-1))
GROUP BY g.run_id
ORDER BY g.run_id
""".format( first_run = seasons_info[season]['first'],
            test_runs = ','.join([str(r) for r in seasons_info[season]['test']]),
            last_run = last_run,
            next_test_runs = ','.join([str(r) for r in next_test_runs]),
            dataset_id = args.dataset_id
)

        logger.debug('SQL: {0}'.format(sql))

        RunInfo = fdb.fetchall(sql, UseDict=True)

#    logger.debug("RunInfo = %s" % str(RunInfo))

    if args.force_validation is not None:
        logger.info('--force-validation enabled: {}'.format(args.force_validation))

    main(RunInfo, logger, season, args)

    logger.info('Post processing completed')

