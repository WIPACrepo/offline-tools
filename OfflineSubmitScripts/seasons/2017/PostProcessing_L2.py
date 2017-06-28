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
from libs.files import tar_gaps_files, insert_gaps_file_info_into_db, tar_log_files, create_good_run_list, MetaXMLFile
from libs.trimrun import trim_to_good_run_time_range
from libs.path import make_relative_symlink, get_logdir, get_tmpdir
from libs.utils import Counter, DBChecksumCache

def validate_run(dataset_id, run, args, iceprod, logger, counter, checksumcache):
    config = get_config(logger)

    logger.info(run.format("======= Checking run {run_id}, production_version {production_version}, dataset_id = {dataset_id} ===========", dataset_id = dataset_id))

    # Is already validated?
    if run.is_validated(dataset_id):
        if args.re_validate:
            logger.info('Re-validate this run.')
        else:
            counter.count('skipped')
            logger.info('Run has already been validated. Skip this run.')
            return

    # Check run status
    run_status = iceprod.get_run_status(dataset_id, run)
    if run_status != 'OK':
        logger.warning('The run has not been successfully processed yet (status: {0}). Skip this run.'.format(run_status))
        counter.count('skipped')
        return

    if not validate_files(iceprod, dataset_id, run, checksumcache, logger):
        logger.error(run.format('Files validation failed for run {run_id}, production_version = {production_version}'))
        counter.count('error')
        return

    logger.info("Files validated")

    # Write gap file info into filter-db
    # This is important at this step to have al subrun information in orde rto trim the run
    logger.info('Insert gaps file info into DB')
    insert_gaps_file_info_into_db(run, args.dryrun, logger)

    # Update run data
    run.get_start_time(force_reload = True)

    ## delete/trim files when food start/stop differ from run start/stop
    logger.info('Check if run is in good start/stop time range')
    trim_to_good_run_time_range(iceprod, dataset_id, run, logger, args.dryrun, not args.not_already_trimmed)

    # Now, we do this a second time: the trim function may have recreated a gaps file
    logger.info('Insert additional gaps file info into DB')
    gaps_files = insert_gaps_file_info_into_db(run, args.dryrun, logger)

    # Tar gaps files
    logger.info('Tar gaps files and add to file catalog')
    tar_gaps_files(iceprod, dataset_id, run, logger, args.dryrun)

    # Delete the gaps files
    logger.info('Remove {0} gaps files'.format(len(gaps_files)))
    for f in gaps_files:
        logger.debug('Remove {0}'.format(f.path))
        if not args.dryrun:
            os.remove(f.path)

    logger.debug('Get active DOMs/string information from GCD file and store it in DB')
    run.write_active_x_to_db()

    if not args.nometadata:
        dest_folder = ''
        if args.dryrun:
            meta_file_dest = get_tmpdir()
        else:
            meta_file_dest = run.format(config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER'))

        metafile = MetaXMLFile(meta_file_dest, run, 'L2', dataset_id, logger)
        metafile.add_post_processing_info(__file__)
    else:
        logger.info("No meta data files will be written")

    logger.debug('tar log files')
    tar_log_files(run, logger, args.dryrun)

    if not run.is_test_run():
        logger.info('Create run sym link')
        make_relative_symlink(run.format(config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER')), run.format(config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER_LINK')), args.dryrun, logger, replace = True)

    logger.info('Mark as validated')
    run.set_post_processing_state(dataset_id, True)

    counter.count('validated')

    logger.info("Checks passed")

def main(args, run_ids, config, logger):
    db = DatabaseConnection.get_connection('filter-db', logger)
    iceprod = IceProd1(logger, args.dryrun)
    checksumcache = DBChecksumCache(logger, dryrun = args.dryrun)

    counter = Counter(['handled', 'validated', 'skipped', 'error'])

    # If no runs have been specified, find all runs of current season_info
    # Current season is specified in config file at DEFAULT:Season
    if not len(run_ids):
        season = config.getint('DEFAULT', 'Season')
        info = config.get_season_info()

        if season not in info:
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
            FROM i3filter.run
            WHERE (run_id BETWEEN {first} AND {last} OR
                run_id IN ({test_runs})) AND
                run_id NOT IN ({excluded_runs})'''.format(
                first = info[season]['first'],
                last = upper_limit,
                test_runs = info[season]['test'],
                excluded_runs = excluded_runs
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
            runs.append(r)
        except LoadRunDataException:
            logger.warning('Skipping run {0} since there are no DB entries'.format(run_id))
            counter.count('skipped')

    logger.debug('Runs: {0}'.format(runs))

    datasets = set()

    for run in runs:
        counter.count('handled')

        try:
            # Get the dataset id and season for this run
            dataset_id = args.dataset_id
            if dataset_id is None:
                dataset_id = config.get_dataset_id_by_run(run.run_id)

                if len(dataset_id) != 1:
                    logger.critical('Did not find exactly one dataset id for run {0}: {1}'.format(run.run_id, dataset_id))
                    raise Exception('Did not find exactly one dataset id for run {0}: {1}'.format(run.run_id, dataset_id))

                dataset_id = dataset_id[0]

            datasets.add(dataset_id)

            validate_run(dataset_id, run, args, iceprod, logger, counter, checksumcache)
        except Exception as e:
            counter.count('error')
            logger.exception(run.format("Exception {e} thrown for run = {run_id}, production_version = {production_version}", e = e))
   
    if len(datasets) > 1:
        logger.warning("We have runs from more than one dataset: {0}".format(datasets))

    # Create run info files for all dataset ids thta are affected
    for dataset_id in datasets:
        create_good_run_list(dataset_id, db, logger, args.dryrun)

    logger.info('Post processing complete: {0}'.format(counter.get_summary()))

def create_grl_only(config, args):
    dataset_id = args.dataset_id

    if dataset_id is None:
        season = confog.get('DEFAULT', 'Season')
        datasets = config.get_datasets_info()
    
        dataset_ids = [d['dataset_id'] for d in datasets if d['type'] == 'L2' and d['season'] == season]
    
        if len(dataset_ids) > 1:
            logger.critical('There are more than one dataset id for the current season. Please specify the dataset id.')
            exit(1)
    
        if not len(dataset_ids):
            logger.critical('Did not find any dataset id for the current season')
            exit(1)

        dataset_id = dataset_ids[0]

    db = DatabaseConnection.get_connection('filter-db', logger)
    create_good_run_list(dataset_id, db, logger, args.dryrun)

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)
    parser.add_argument("--dataset-id", type = int, required = False, default = None, help="The dataset id. The default value is `None`. In this case it gets the dataset id from the database.")
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start submitting from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End submitting at this run")
    parser.add_argument("--runs", type = int, nargs = '*', required = False, help = "Submitting specific runs. Can be mixed with -s and -e")
    parser.add_argument("--nometadata", action = "store_true", default = False, help="Do not write meta data files")
    parser.add_argument("--cron", action = "store_true", default = False, help = "Use this option if you call this script via a cron")
    parser.add_argument("--re-validate", action = "store_true", default = False, help = "Also validate runs that have already been validated")
    parser.add_argument("--create-grl-only", action = "store_true", default = False, help = "Do not validate runs. Just create the GRL for the current season")
    parser.add_argument("--not-already-trimmed", action = "store_true", default = False, help = "If the data files aren't already trimmed to the right size (is automatically done since V05-01-06) activate this option")
    args = parser.parse_args()

    logfile = os.path.join(get_logdir(sublogpath = 'PostProcessing'), 'PostProcessing_')

    if args.cron:
        logfile += 'CRON_'

    if args.logfile is not None:
        logfile = args.logfile

    logger = get_logger(args.loglevel, logfile)

    config = get_config(logger)

    if args.create_grl_only:
        create_grl_only(config, args)
        exit()

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
        if not config.getboolean('Level2', 'CronPostProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = Lock(os.path.basename(__file__), logger)
        lock.lock()

    main(args, runs, config, logger)

    if args.cron:
        lock.unlock()

    logger.info('Done')

