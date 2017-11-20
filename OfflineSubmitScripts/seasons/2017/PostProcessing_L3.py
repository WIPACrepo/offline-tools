#!/usr/bin/env python
"""
Combines several checks wich have to be done after the
files are generated and updates the databases accordingly.
"""

import os

from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.config import get_config
from libs.process import Lock
from libs.databaseconnection import DatabaseConnection
from libs.runs import Run, LoadRunDataException
from libs.iceprod1 import IceProd1
from libs.postprocessing import validate_files
from libs.files import tar_gaps_files, insert_gaps_file_info_into_db, tar_log_files, MetaXMLFile
from libs.trimrun import trim_to_good_run_time_range
from libs.path import make_relative_symlink, get_logdir, get_tmpdir
from libs.utils import Counter, DBChecksumCache
from libs.cron import cron_finished

def validate_run(source_dataset_ids, run, args, iceprod, logger, counter, checksumcache):
    config = get_config(logger)

    # Find source dataset_id for this run
    source_dataset_id = None
    for sd_id in source_dataset_ids:
       if iceprod.get_run_status(sd_id, run) == 'OK':
            source_dataset_id = sd_id
            break

    if source_dataset_id is None:
        logger.error(run.format('Could not determine source dataset id of run {run_id}'))
        counter.count('skipped')
        return

    logger.info(run.format("======= Checking run {run_id}, source dataset = {source_dataset_id}, destination dataset = {destination_dataset_id} ===========", source_dataset_id = source_dataset_id, destination_dataset_id = args.destination_dataset_id))

    # Is already validated?
    if run.is_validated(args.destination_dataset_id):
        if args.re_validate:
            logger.info('Re-validate this run.')
        else:
            counter.count('skipped')
            logger.info('Run has already been validated. Skip this run.')
            return

    # Check run status
    run_status = iceprod.get_run_status(args.destination_dataset_id, run)
    if run_status != 'OK':
        logger.warning('The run has not been successfully processed yet (status: {0}). Skip this run.'.format(run_status))
        counter.count('skipped')
        return

    if not validate_files(iceprod, args.destination_dataset_id, run, checksumcache, logger, level = 'L3'):
        logger.error(run.format('Files validation failed for run {run_id}, production_version = {production_version}'))
        counter.count('error')
        return

    logger.info("Files validated")

    # Get L3 run folder
    run_folder = run.format(config.get_level3_info()[int(args.destination_dataset_id)]['path'])

    if not args.nometadata:
        dest_folder = ''
        if args.dryrun:
            meta_file_dest = get_tmpdir()
        else:
            meta_file_dest = run_folder

        metafile = MetaXMLFile(meta_file_dest, run, 'L3', args.destination_dataset_id, logger)
        metafile.add_post_processing_info(__file__, args.no_svn)
    else:
        logger.info("No meta data files will be written")

    logger.debug('tar log files')
    tar_log_files(run, logger, args.dryrun, run_folder = run_folder)

    logger.info('Mark as validated')
    run.set_post_processing_state(args.destination_dataset_id, True)

    counter.count('validated')

    logger.info("Checks passed")

def main(args, run_ids, config, logger):
    db = DatabaseConnection.get_connection('filter-db', logger)
    iceprod = IceProd1(logger, args.dryrun)
    checksumcache = DBChecksumCache(logger, dryrun = args.dryrun)

    counter = Counter(['handled', 'validated', 'skipped', 'error'])

    # Source dataset ids
    if args.source_dataset_id is not None:
        logger.info('Source dataset id has been explicitely set through the parameter!')
        source_dataset_ids = [args.source_dataset_id]
    else:
        source_dataset_ids = config.get_source_dataset_ids(args.destination_dataset_id)

    if not len(source_dataset_ids):
        logger.critical('Did not find source dataset id for dataset id {0}'.format(args.destination_dataset_id))
        exit(1)

    # If no runs have been specified, find all runs of current season_info
    # Current season is specified in config file at DEFAULT:Season
    if not len(run_ids):
        season = config.getint('DEFAULT', 'Season')
        info = config.get_seasons_info()

        if int(season) not in info:
            logger.critical('Did not find information about season {0}'.format(season))
            exit(1)

        excluded_runs = [-1]
        upper_limit = 99999999

        next_season = season + 1
        if next_season not in info:
            logger.info('Next season has not been configured yet. No information about test runs.')
        else:
            excluded_runs.extend(info[next_season]['test'])
            upper_limit = info[next_season]['first'] - 1

        sql = '''
            SELECT run_id
            FROM i3filter.runs
            WHERE (run_id BETWEEN {first} AND {last} OR
                run_id IN ({test_runs})) AND
                run_id NOT IN ({excluded_runs})'''.format(
                first = info[season]['first'],
                last = upper_limit,
                test_runs = ','.join(str(e) for e in info[season]['test']),
                excluded_runs = ','.join(str(e) for e in excluded_runs)
            )

        logger.debug('SQL: {0}'.format(sql))

        query = db.fetchall(sql)

        run_ids = [r['run_id'] for r in query]

    logger.debug('Run IDs: {0}'.format(run_ids))

    runs = []
    for run_id in run_ids:
        try:
            r = Run(run_id, logger, dryrun = args.dryrun)
            r.load()

            if r.is_good_run():
                runs.append(r)
        except LoadRunDataException:
            logger.warning('Skipping run {0} since there are no DB entries'.format(run_id))
            counter.count('skipped')

    logger.debug('Runs: {0}'.format(runs))

    for run in runs:
        counter.count('handled')

        try:
            validate_run(source_dataset_ids, run, args, iceprod, logger, counter, checksumcache)
        except Exception as e:
            counter.count('error')
            logger.exception(run.format("Exception {e} thrown for run = {run_id}, production_version = {production_version}", e = e))
   
    logger.info('Post processing complete: {0}'.format(counter.get_summary()))
    return counter

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)
    parser.add_argument("--source-dataset-id", type = int, required = False, default = None, help="Dataset ID to read from, usually L2 dataset. Use this option only if you want override the configuration.")
    parser.add_argument("--destination-dataset-id", type = int, required = True, default = None, help="Dataset ID to write to, usually L3 dataset")
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start submitting from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End submitting at this run")
    parser.add_argument("--runs", type = int, nargs = '*', required = False, help = "Submitting specific runs. Can be mixed with -s and -e")
    parser.add_argument("--nometadata", action = "store_true", default = False, help="Do not write meta data files")
    parser.add_argument("--cron", action = "store_true", default = False, help = "Use this option if you call this script via a cron")
    parser.add_argument("--re-validate", action = "store_true", default = False, help = "Also validate runs that have already been validated")
    parser.add_argument("--no-svn", action = "store_true", default = False, help = "No SVN is available. No SVN information will be logged.")
    parser.add_argument("--cosmicray", action = "store_true", default = False, help= " Important if you submit L3 jobs for the cosmic ray WG")
    parser.add_argument("--aggregate", type = int, default = None, help = "USE THIS OPTION IF YOU WANT TO OVERRIDE THE DATASET CONFIGURATION ONLY. DO USE THIS OPTION ONLY IF YOU KNOW WHAT YOU ARE DOING. Number of subruns to aggregate to form one job, needed when processing 1 subrun is really short.")
    args = parser.parse_args()

    logfile = os.path.join(get_logdir(sublogpath = 'L3Processing'), 'PostProcessing_')

    if args.cron:
        logfile += 'CRON_'

    if args.logfile is not None:
        logfile = args.logfile

    logger = get_logger(args.loglevel, logfile, svn_info_from_file = args.no_svn)

    config = get_config(logger)

    # Check arguments
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

    if not len(runs):
        logger.info('No specific runs have been set. Going to validate all runs of current season (check config file DEFAULT:Season).')

    # Check if --cron option is enabled. If so, check if cron usage allowed by config
    lock = None
    if args.cron:
        if not config.getboolean('Level3', 'CronPostProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = Lock(
            os.path.basename(__file__),
            logger,
            lock_file = os.path.join(get_tmpdir(), os.path.splitext(os.path.basename(__file__))[0] + '-' + str(args.destination_dataset_id) + '.lock')
        )
        lock.lock()

    counter = main(args, runs, config, logger)

    if args.cron:
        lock.unlock()
        cron_finished(os.path.basename(__file__), counter, logger, args.dryrun)

    logger.info('Done')

