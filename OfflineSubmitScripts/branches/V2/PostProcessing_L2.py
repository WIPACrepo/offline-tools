#!/usr/bin/env python
"""
Combines several checks wich have to be done after the
files are generated and updates the databases accordingly.
"""

from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.config import get_config
from libs.process import Lock
from libs.databaseconnection import DatabaseConnection
from libs.runs import Run
from libs.iceprod1 import IceProd1
from libs.postprocessing import validate_files
from libs.files import tar_gaps_files, insert_gaps_file_info_into_db
from libs.trimrun import trim_to_good_run_time_range

def validate_run(dataset_id, run, args, iceprod, logger):
    logger.info(run.format("======= Checking run {run_id}, production_version {production_version}, dataset_id = {dataset_id} ===========", dataset_id = dataset_id))

    # Check run status
    run_status = iceprod.get_run_status(dataset_id, run)
    if run_status != 'OK':
        logger.warning('The run has not been successfully processed yet (status: {0}). Skip this run.'.format(run_status))
        return

    if not validate_files(iceprod, dataset_id, run, logger)
        logger.error(run.format('Files validation failed for run {run_id}, production_version = {production_version}'))
        return

    logger.info("Files validated")

    logger.info('Tar gaps files and add to file catalog')
    tar_gaps_files(iceprod, dataset_id, run, logger, args.dryrun)

    # Write gap file info into filter-db
    logger.info('Insert gaps file info into DB')
    gaps_files = insert_gaps_file_info_into_db(run, args.dryrun, logger)

    # Delete the gaps files
    logger.info('Remove {0} gaps files'.format(len(gaps_files)))
    for f in gaps_files:
        logger.debug('Remove {0}'.format(f))
        if not args.dryrun:
            os.remove(f)

    ## delete/trim files when food start/stop differ from run start/stop
    trim_to_good_run_time_range(iceprod, dataset_id, run, logger, args.dryrun)
    logger.info('Check if run is in good start/stop time range')

    # TODO
    logger.info( "--Attempting to collect Active Strings/DOMs information from verified GCD file ...")

    R = RunTools(r['run_id'])
    if 1 == R.GetActiveStringsAndDoms(season, UpdateDB = not dryrun):
        logger.error("GetActiveStringsAndDoms failed")
        return

    if not dryrun: dbs4_.execute("""update i3filter.grl_snapshot_info 
                         set validated=1
                         where run_id=%s and production_version=%s"""%\
                     (r['run_id'],str(r['production_version'])))

    sDay = r['tStart']
    sY = sDay.year
    sM = str(sDay.month).zfill(2)
    sD = str(sDay.day).zfill(2)

    run_folder = "/data/exp/IceCube/%s/filtered/level2/%s%s/Run00%s_%s" % (sY, sM, sD, r['run_id'], r['production_version'])

    if not nometadata:
        dest_folder = ''
        if dryrun:
            dest_folder = get_tmpdir()
        else:
            dest_folder = run_folder

        write_meta_xml_post_processing(dest_folder = dest_folder,
                                       level = 'L2',
                                       script_file = __file__,
                                       logger = logger)
    else:
        logger.info("No meta data files will be written")

    logger.debug('tar log files')

    tar_log_files(run_path = run_folder, dryrun = dryrun, logger = logger)

    # TODO: Make symlink to run folder
    # TODO: Mark as validated

    logger.info("Checks passed")
    logger.info("======= End Checking %i %i ======== " %(r['run_id'],r['production_version'])) 
    return

def main(args, runs, config, logger):
    db = DatabaseConnection.get_connection('filter-db', logger)
    iceprod = IceProd1(logger, args.dryrun)

    # If no runs have been specified, find all runs of current season_info
    # Current season is specified in config file at DEFAULT:Season
    if not len(runs):
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

        runs = [r['run_id'] for r in query]

    runs = [Run(run_id, logger, dryrun = args.dryrun) for run_id in runs]
    datasets = set()

    for run in runs:
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

            validate_run(dataset_id, run, args, iceprod, logger) 
        except Exception as e:
            logger.exception(run.format("Exception {0} thrown for run = {run_id}, production_version = {production_version}"))
   
    if len(datasets) > 1:
        logger.warning("We have runs from more than one dataset: {0}".format(datasets))

    # Create run info files for all dataset ids thta are affected
    for dataset_id in datasets:
        create_good_run_list(dataset_id, db, logger, args.dryrun)

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)
    parser.add_argument("--dataset-id", type = int, required = False, default = None, help="The dataset id. The default value is `None`. In this case it gets the dataset id from the database.")
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start submitting from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End submitting at this run")
    parser.add_argument("--runs", type = int, nargs = '*', required = False, help = "Submitting specific runs. Can be mixed with -s and -e")
    parser.add_argument("--nometadata", action = "store_true", default = False, help="Do not write meta data files")
    parser.add_argument("--cron", action = "store_true", default = False, help = "Use this option if you call this script via a cron")
    args = parser.parse_args()

    logfile = os.path.join(get_logdir(sublogpath = 'PostProcessing'), 'PostProcessing_')

    if args.logfile is not None:
        logfile = args.logfile

    logger = get_logger(args.loglevel, logfile)

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
        if not config.getboolean('Level2', 'CronPostProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = Lock(os.path.basename(__file__), logger)
        lock.lock()

    main(args, runs, config, logger)

    if args.CRON:
        lock.unlock()

    logger.info('Post processing completed')

