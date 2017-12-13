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

import glob

import libs.files
from libs.config import get_season_info, get_config, get_dataset_id_by_run, get_season_by_run
sys.path.append(get_config().get('DEFAULT', 'SQLClientPath'))
sys.path.append(get_config().get('DEFAULT', 'ProductionToolsPath'))
from RunTools import RunTools
from FileTools import *
from DbTools import *

from libs.files import get_tmpdir, get_logdir, MakeTarGapsTxtFile, MakeRunInfoFile, write_meta_xml_post_processing, tar_log_files, insert_gap_file_info_and_delete_files, GapsFile, lost_file_info
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.checks import CheckFiles
import libs.process
from GoodRuntimeAdjust import main as GoodRuntimeAdjust

from libs.databaseconnection import DatabaseConnection

import SQLClient_i3live as live
import SQLClient_dbs4 as dbs4
import SQLClient_dbs2 as dbs2

m_live = live.MySQL()    
dbs4_ = dbs4.MySQL()   
dbs2_ = dbs2.MySQL()    

from icecube import dataio, dataclasses, icetray

def remove_empty_files(run_folder, dataset_id, run_id, logger, dryrun):
    files = glob.glob(os.path.join(run_folder, 'Level2pass2_IC*_data_Run00*_Subrun00000000_00000*_gaps.txt'))
    files = [f for f in files if not os.path.getsize(f)]

    if len(files):
        logger.warning('Found empty gaps files')
    else:
        logger.info('No files to remove')
        return True

    data_files = [f.replace('_gaps.txt', '.i3.zst') for f in files]

    for f in data_files:
        logger.info('Check {0}'.format(f))

        o = dataio.I3File(f)

        only_i_frame = True

        while o.more():
            frame = o.pop_frame()

            if frame.Stop != icetray.I3Frame.TrayInfo:
                only_i_frame = False
                break

        o.close()

        if not only_i_frame:
            logger.error('Found empty gaps file but data file contains more than just I-frames: {0}'.format(f))
            return False

        # Ok, remove files
        rfiles = glob.glob(f.replace('.i3.zst', '') + '*')

        for rf in rfiles:
            logger.info('Remove {0}'.format(rf))

            sql = """
                UPDATE i3filter.urlpath u
                    JOIN i3filter.run r USING(dataset_id, queue_id)
                    JOIN i3filter.job j USING(dataset_id, queue_id)
                SET u.transferstate = "IGNORED", j.status = 'BadRun'
                WHERE dataset_id = {dataset_id} AND r.run_id = {run_id} AND u.name LIKE '{filenames}'
            """.format(dataset_id = dataset_id, run_id = run_id, filenames = os.path.basename(f).replace('.i3.zst', '') + '%')

            logger.debug('SQL: {0}'.format(sql))

            if not dryrun:
                os.remove(rf)
                dbs4_.execute(sql)

    return True

def get_sub_run_info_pass1(runs, logger):
    db = DatabaseConnection.get_connection('filter-db', logger)

    if not isinstance(runs, list):
        runs = [runs]
    
    sql = "SELECT * FROM sub_runs WHERE run_id IN (%s) AND NOT bad ORDER BY run_id, sub_run" % ','.join([str(r) for r in runs])

    dbdata = db.fetchall(sql, UseDict = True)

    data = {}
    for row in dbdata:
        if row['run_id'] not in data:
            data[row['run_id']] = {}

        row['first_event'] = dataclasses.I3Time(row['first_event_year'], row['first_event_frac'])
        row['last_event'] = dataclasses.I3Time(row['last_event_year'], row['last_event_frac'])

        data[row['run_id']][row['sub_run']] = row 

    return data

def main_run(r, logger, dataset_id, season, nometadata, dryrun = False, no_pass2_gcd_file = False, npx = False, update_active_X_only = False, missing_output_files = None, force = None, accelerate = False):
    force_m = force
    force = not (force is None)

    logger.debug('force = {}'.format(force))

    # Lost files
    pass2_lost_files = DatabaseConnection.get_connection('filter-db', logger).fetchall('SELECT * FROM i3filter.missing_files_pass2 WHERE run_id = {0} AND NOT resolved ORDER BY sub_run'.format(r['run_id']), UseDict = True)

    sDay = r['tStart']
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)

    run_folder = "/data/exp/IceCube/%s/filtered/level2pass2/%s%s/Run00%s_%s" % (sY, sM, sD, r['run_id'], r['production_version'])

    logger.info("======= Checking %s %s ==========="  %(str(r['run_id']),str(r['production_version'])))

    if dataset_id < 0:
        logger.error("Cannot determine the dataset id. Skipping.")
        return
    else:
        logger.info("Dataset id was determined to %s" % dataset_id)

    if len(pass2_lost_files):
        logger.warning('We have lost files in run {0}: {1} lost files'.format(r['run_id'], len(pass2_lost_files)))
        for f in pass2_lost_files:
            logger.warning('  {sub_run}: {path}'.format(**f))

    if DbTools(r['run_id'], dataset_id).AllOk():
        logger.warning( """Processing of Run=%s, production_version=%s
                 may not be complete ... skipping"""\
                %(r['run_id'],str(r['production_version'])))
        return
     
    if update_active_X_only:
        logger.warning('*** Only the active strings, active doms and active in ice doms are updated. NO VALIDATION! ***')
    else:
        # check i/o files in data warehouse and Db
        logger.info("Checking Files in Data warehouse and database records ...")
        if CheckFiles(r, logger, dataset_id = dataset_id, season = season, dryrun = dryrun, no_pass2_gcd_file = no_pass2_gcd_file, missing_output_files = missing_output_files, force = force, accelerate = accelerate, pass2_lost_files = pass2_lost_files):
            logger.error("FilesCheck failed: for Run=%s, production_version=%s"\
            %(r['run_id'],str(r['production_version'])))

            if not force:
                return
            else:
                logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')
        logger.info("File checks  .... passed")

        # Remove empty files (they are outside of the good time range
        if not remove_empty_files(run_folder, dataset_id, r['run_id'], logger, dryrun):
            if not force:
                return
            else:
                logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')

        # Check if files cover the period good start/end time (in case a file has been forgotten etc.)
        l2files = [f for f in RunTools(r['run_id'], logger, passNumber = 2).GetRunFiles(r['tStart'], 'L') if f.endswith('_gaps.txt')]
        first_gaps = GapsFile(l2files[0], logger)
        last_gaps = GapsFile(l2files[-1], logger)

        first_gaps.read()
        last_gaps.read()

        first_event_of_first_file = first_gaps.get_first_event()
        last_event_of_last_file = last_gaps.get_last_event()

        first_event_of_first_file = dataclasses.I3Time(first_event_of_first_file['year'], first_event_of_first_file['frac']).date_time
        last_event_of_last_file = dataclasses.I3Time(last_event_of_last_file['year'], last_event_of_last_file['frac']).date_time

        logger.info('Checking good start/stop times')
        # Check if the times deviate for more than 1 second

        # Lost files?
        last_sub_run_id = int(last_gaps.get_sub_run_id())
        appending_lost_files = [f for f in pass2_lost_files if int(f['sub_run']) > last_sub_run_id]
        appending_livetime = float(sum([f['livetime'] for f in appending_lost_files if f['livetime'] is not None]))

        first_sub_run_id = int(first_gaps.get_sub_run_id())
        prepending_lost_files = [f for f in pass2_lost_files if int(f['sub_run']) < first_sub_run_id]
        prepending_livetime = float(sum([f['livetime'] for f in prepending_lost_files if f['livetime'] is not None]))

        if len(prepending_lost_files):
            logger.warning('We have lost files at the start of the run:')
            for f in prepending_lost_files:
                logger.warning('  {sub_run}: livetime {livetime}s'.format(**f))

        if len(appending_lost_files):
            logger.warning('We have lost files at the end of the run:')
            for f in appending_lost_files:
                logger.warning('  {sub_run}: livetime {livetime}s'.format(**f))

        try:
            pass1_data = get_sub_run_info_pass1(r['run_id'], logger)[r['run_id']]
            pass1_data_sub_runs = pass1_data.keys()
        except KeyError:
            logger.warning('Could not find pass1 run data. That is probably caused by good_it = 1 and good_i3 = 0. Those runs were not processed in 2011/pass1. If an error occurs, you need to check manually.')
            pass1_data = None
            pass1_data_sub_runs = None

        # *_frac is in tenth of nanoseconds
        precise_good_start_time = r['good_tstart'].replace(microsecond = (r['good_tstart_frac'] if r['good_tstart_frac'] is not None else 0) / 10 / 1000)
        precise_good_stop_time = r['good_tstop'].replace(microsecond = (r['good_tstop_frac'] if r['good_tstop_frac'] is not None else 0) / 10 / 1000)

        if (precise_good_start_time - first_event_of_first_file).total_seconds() + prepending_livetime < -1:
            logger.error('Probably missing a file or data:')
            logger.error('  good start time:  {0}'.format(precise_good_start_time))
            logger.error('  first file start: {0} - {1}s (lost data) = {2}'.format(first_event_of_first_file, prepending_livetime, first_event_of_first_file - datetime.timedelta(seconds = prepending_livetime)))

            if abs((first_event_of_first_file - pass1_data[pass1_data_sub_runs[0]]['first_event'].date_time).total_seconds() - prepending_livetime) <= 1:
                logger.warning('The file time does not differ more than 1 second from the pass1 time: {}'.format(abs((first_event_of_first_file - pass1_data[pass1_data_sub_runs[0]]['first_event'].date_time).total_seconds() - prepending_livetime)))
                logger.warning('Pass1 file start time: {}'.format(pass1_data[pass1_data_sub_runs[0]]['first_event']))
                logger.warning('This is OK and we keep on validating!')
            else:
                logger.error('The file time does differ more than 1 second from the pass1 time: {}'.format(abs((first_event_of_first_file - pass1_data[pass1_data_sub_runs[0]]['first_event'].date_time).total_seconds() - prepending_livetime)))
                logger.error('Pass1 file start time: {}'.format(pass1_data[pass1_data_sub_runs[0]]['first_event']))
                if not force:
                    return
                else:
                    logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')

        if ((precise_good_stop_time - last_event_of_last_file).total_seconds()) - appending_livetime > 1:
            logger.error('Probably missing a file or data:')
            logger.error('  good stop time:       {0}'.format(precise_good_stop_time))
            logger.error('  last file stop:       {0} + {1}s (lost data)'.format(last_event_of_last_file, appending_livetime))
            logger.error('  Pass1 file stop time: {}'.format(pass1_data[pass1_data_sub_runs[-1]]['last_event']))

            if abs((last_event_of_last_file - pass1_data[pass1_data_sub_runs[-1]]['last_event'].date_time).total_seconds() + appending_livetime) <= 1:
                logger.warning('The file time does not differ more than 1 second from the pass1 time: {}'.format(abs((last_event_of_last_file - pass1_data[pass1_data_sub_runs[-1]]['last_event'].date_time).total_seconds())))
                logger.warning('Pass1 file stop time: {}'.format(pass1_data[pass1_data_sub_runs[-1]]['last_event']))
                logger.warning('This is OK and we keep on validating!')
            else:
                if not force:
                    return
                else:
                    logger.warning('IGNORE ERROR SINCE --force-validation IS ENABLED')

        logger.debug("--Attempting to tar _gaps.txt files ...")
        MakeTarGapsTxtFile(dbs4_, r['tStart'], r['run_id'], datasetid = dataset_id, dryrun = dryrun, logger = logger)
        logger.debug("MakeTarGapsFile              .... passed")

    logger.info( "--Attempting to collect Active Strings/DOMs information from verified GCD file ...")

    R = RunTools(r['run_id'], passNumber = 2, logger = logger)
    if 1 == R.GetActiveStringsAndDoms(season, UpdateDB = not dryrun):
        logger.error("GetActiveStringsAndDoms failed")
        return

    if not update_active_X_only:
        if not dryrun: dbs4_.execute("""update i3filter.grl_snapshot_info_pass2 
                             set validated=1
                             where run_id=%s and production_version=%s"""%\
                         (r['run_id'],str(r['production_version'])))

        sql = """   INSERT INTO post_processing
                        (run_id, dataset_id, validated, date_of_validation)
                    VALUES
                        (%s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        validated = %s,
                        date_of_validation = NOW()
                        """ % (r['run_id'], dataset_id, 1, 1)

        logger.debug("SQL: %s" % sql)

        filter_db = DatabaseConnection.get_connection('filter-db', logger)

        if not dryrun:
            filter_db.execute(sql)

        if not nometadata:
            dest_folder = ''
            if dryrun:
                dest_folder = get_tmpdir()
            else:
                dest_folder = run_folder

            write_meta_xml_post_processing(dest_folder = dest_folder,
                                           level = 'L2',
                                           script_file = __file__,
                                           logger = logger,
                                           npx = npx)
        else:
            logger.info("No meta data files will be written")

        logger.debug('tar log files')

        tar_log_files(run_path = run_folder, dryrun = dryrun, logger = logger)

        # Write gap file info into filter-db and get rid of the gaps files
        insert_gap_file_info_and_delete_files(run_path = run_folder, dryrun = dryrun, logger = logger)

        # Write lost file info
        lost_file_info(run_folder, pass2_lost_files, logger, dryrun)

        if force or len(pass2_lost_files):
            sql = '''INSERT INTO i3filter.run_comments (run_id, snapshot_id, production_version, `pass`, `date`, add_to_grl, `comment`)
                     VALUES (%(run_id)s, %(snapshot_id)s, %(production_version)s, 2, NOW(), %(add_to_grl)s, %(comment)s)'''

            logger.debug('SQL: %s' % sql)

            if force_m is None:
                force_m = ''
            else:
                force_m += ' '

            if len(pass2_lost_files):
                force_m += 'Has lost files.'

            if not dryrun:
                filter_db.execute(sql, params = {
                    'comment': 'Validated manually. ' + force_m,
                    'run_id': r['run_id'],
                    'snapshot_id': r['snapshot_id'],
                    'production_version': r['production_version'],
                    'add_to_grl': 1 if len(pass2_lost_files) else 0
                })

        logger.info("Checks passed")
    logger.info("======= End Checking %i %i ======== " %(r['run_id'],r['production_version'])) 
    return

def main(runinfo, logger, nometadata, dryrun = False, no_pass2_gcd_file = False, npx = False, update_active_X_only = False, force = None, accelerate = False):
    datasets = set()

    missing_output_files = []

    for run in runinfo:
        try:
            # Get the dataset id and season for this run
            dataset_id = get_dataset_id_by_run(int(run['run_id']))
            season = get_season_by_run(run['run_id']);

            # If the dataset id is good, add it to the list of processed dataset ids
            if dataset_id > 0:
                datasets.add(dataset_id)
                logger.debug('Add dataset {}'.format(dataset_id))
            
            main_run(run, logger, dataset_id = dataset_id, season = season, nometadata = nometadata, dryrun = dryrun, no_pass2_gcd_file = no_pass2_gcd_file, npx = npx, update_active_X_only = update_active_X_only, missing_output_files = missing_output_files, force = force, accelerate = accelerate)
        except Exception as e:
            logger.exception("Exception %s thrown for: Run=%s, production_version=%s" %(e.__repr__(),run['run_id'],str(run['production_version'])))
   
    if len(datasets) > 1:
        logger.warning("We have runs from more than one dataset: %s" % datasets)
    elif not len(datasets):
        logger.error('We have no datasets...?')

    if runinfo:
        # Create run info files for all dataset ids thta are affected
        for dataset in datasets:
            MakeRunInfoFile(dbs4_, dataset_id = dataset, logger = logger, dryrun = dryrun) 

    if missing_output_files is not None:
        if len(missing_output_files):
            # Creating a SQL in order to restart the jobs:
            dqlist = []

            logger.info('Missing output files: {}'.format(len(missing_output_files)))

            for f in missing_output_files:
                sql = 'SELECT DISTINCT queue_id FROM i3filter.urlpath WHERE dataset_id = {dataset_id} AND name LIKE \'%{run_id}%\''.format(dataset_id = get_dataset_id_by_run(int(f['run_id'])), run_id = f['run_id'])

                queue_id = dbs4_.fetchall(sql, UseDict = True)

                if len(queue_id) > 1:
                    logger.error('Only one row expected: {}'.format(sql))
                    return

                dqlist.append({'queue_id': queue_id[0]['queue_id'], 'dataset_id': get_dataset_id_by_run(int(f['run_id']))})

            # Create the SQL commands:
            for e in dqlist:
                logger.info('UPDATE i3filter.job SET status = \'WAITING\', failures = 0 WHERE dataset_id = {dataset_id} AND queue_id = {queue_id}'.format(**e))


if __name__ == '__main__':
    parser = get_defaultparser(__doc__,dryrun=True)
    parser.add_argument('--runs', nargs="*", help="Runs to postprocess" ,type = int)
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start post processing from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End post processing at this run")
    parser.add_argument("--nometadata", action="store_true", default=False, dest="NOMETADATA", help="Don't write meta data files")
    parser.add_argument("--no-pass2-gcd-file", action="store_true", default=False, help="If there is no special pass2 GCD file for this runs or season, use this option. Then it looks at the pass1 folder for the verified GCDs.")
    parser.add_argument("--cron", action="store_true", default=False, dest="CRON", help="Use this option if you call this script via a cron")
    parser.add_argument("--force-validation", nargs='+', default = None, required = None, type = str, help = "DO ONLY USE THIS IF YOU KNOW THAT THE ERRORS ARE WRONG. Validates the run(s) despite there are errors. Makes an entry into filter-db.run_comments. If you use this flag, the argument will be used as comment. E.g. '--force-validation \"run times are OK\"'. This will lead to a comment like: \"Validated manually. run times are OK\"")
    parser.add_argument("--npx", action="store_true", default=False, help="Use this option if you let run this script on NPX")
    parser.add_argument("--skip-checksumcheck-and-stream-error-check", action="store_true", default=False, help="Use this option if you want to accelerate the Post Processing. Usfull in combination with --dryrun")
    parser.add_argument("--update-active-X-only", action="store_true", default=False, help="Do only recalculate the active strings, active doms and active in ice doms and recreate the GRL")
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(sublogpath = 'PostProcessing'), 'PostProcessing_')

    if args.force_validation is not None:
        args.force_validation = ' '.join(args.force_validation)

    print 'message: %s' % args.force_validation

    if args.CRON:
        LOGFILE = LOGFILE + 'CRON_'

    logger = get_logger(args.loglevel, LOGFILE, args.npx)

    # Check if --cron option is enabled. If so, check if cron usage allowed by config
    lock = None
    if args.CRON:
        if not get_config().getboolean('L2', 'CronPostProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = libs.process.Lock(os.path.basename(__file__), logger)
        lock.lock()

    RunInfo = None

    runs = args.runs

    if runs is None:
        runs = []

    if args.startrun is not None:
        if args.endrun is None:
            logger.critical('If --startrun, -s has been set, also the --endrun, -e needs to be set.')
            exit(1)

        runs.extend(range(args.startrun, args.endrun + 1)) 
    elif args.endrun is not None:
        logger.critical('If --endrun, -e has been set, also the --startrun, -s needs to be set.')
        exit(1)

    logger.debug('Runs: %s' % runs)

    if len(runs) > 0:
        RunInfo = dbs4_.fetchall("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info_pass2 g
                                  JOIN i3filter.run_info_summary_pass2 r ON r.run_id=g.run_id
                                  WHERE g.submitted AND (g.good_i3 OR g.good_it) AND %s validated AND g.run_id IN (%s)
                                  ORDER BY g.run_id""" % ('' if args.update_active_X_only else 'NOT', ', '.join([str(r) for r in runs])), UseDict=True)
    else: 
        RunInfo = dbs4_.fetchall("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info_pass2 g
                                 JOIN i3filter.run_info_summary_pass2 r ON r.run_id=g.run_id
                                 WHERE g.submitted AND (g.good_i3 OR g.good_it) AND %s validated
                                 ORDER BY g.run_id""" % '' if args.update_active_X_only else 'NOT', UseDict=True)

    logger.debug("""SELECT r.tStart,g.* FROM i3filter.grl_snapshot_info_pass2 g
                                  JOIN i3filter.run_info_summary_pass2 r ON r.run_id=g.run_id
                                  WHERE g.submitted AND (g.good_i3 OR g.good_it) AND %s validated AND g.run_id IN (%s)
                                  ORDER BY g.run_id""" % ('' if args.update_active_X_only else 'NOT', ', '.join([str(r) for r in runs])))

    logger.debug("RunInfo = %s" % str(RunInfo))

    main(RunInfo, logger, args.NOMETADATA, dryrun = args.dryrun, no_pass2_gcd_file = args.no_pass2_gcd_file, npx = args.npx, update_active_X_only = args.update_active_X_only, force = args.force_validation, accelerate = args.skip_checksumcheck_and_stream_error_check)

    if args.CRON:
        lock.unlock()

    logger.info('Post processing completed')

