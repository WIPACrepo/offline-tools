#!/usr/bin/env python

import os

from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.iceprod2 import IceProd2
from libs.config import get_config
from libs.runs import Run, LoadRunDataException
from libs.files import clean_datawarehouse, MetaXMLFile, has_subrun_dstheader_within_good_time_range, File
from libs.path import make_relative_symlink, get_logdir, get_tmpdir
from libs.databaseconnection import DatabaseConnection
from libs.utils import Counter, DBChecksumCache
from libs.process import Lock
from libs.cron import cron_finished

def main(args, run_ids, logger):
    config = get_config(logger)

    counter = Counter(['handled', 'submitted', 'skipped', 'error'])

    # Determine dataset ids:
    run_id_dataset_id_mapping = {}
    for run_id in run_ids:
        dataset_id = args.dataset_id

        if dataset_id is None:
            dataset_id = config.get_dataset_id_by_run(run_id, 'L2')
            

            if len(dataset_id) > 1:
                logger.critical('Run {0} has more than one L2 dataset ids: {1}. Specify the dataset id when running the script to solve this problem.'.format(run_id, dataset_id))
                exit(1)

            if len(dataset_id) == 0:
                logger.critical('No dataset id could be determined for run {0}. Check the database.'.format(run_id))
                exit(1)

            dataset_id = dataset_id[0]

        dataset_info = config.get_dataset_info(dataset_id)
        logger.info(dataset_info)
        run_id_dataset_id_mapping[run_id] = dataset_info

    logger.debug('Dataset id mapping: {0}'.format(run_id_dataset_id_mapping))

    # Create Run objects and filter bad runs
    runs = []
    for run_id in run_ids:
        try:
            r = Run(run_id, logger, dryrun = args.dryrun)
            r.load()
            runs.append(r)
        except LoadRunDataException:
            logger.warning('Skipping run {0} since there are no DB entries'.format(run_id))
            counter.count('skipped')

    # Submit only good runs or test runs (if not failed)
    runs = [run for run in runs if run.is_good_run() or (run.is_test_run() and not run.is_failed_run())]

    authtok = config.get('ip2auth','rotok')
    iceprod = IceProd2(logger, args.dryrun, args.username, authtok)

    # Check if the resubmission flag has been set and if all runs are due for a resubmission
    ignored_runs = []
    for run in runs:
        if iceprod.is_run_submitted(run_id_dataset_id_mapping[run.run_id]['dataset_id'], run):
            if args.resubmission:
                logger.warning(run.format('Run {run_id} will be resubmitted.'))
            else:
                logger.warning(run.format('Run {run_id} has already been submitted. The --resubmission flag has not been set. This run will be skipped.'))
                ignored_runs.append(run)

    checksumcache = DBChecksumCache(logger, dryrun = args.dryrun)

    for run in runs:
        counter.count('handled')

        if run in ignored_runs:
            counter.count('skipped')
            continue

        try:
            # Dataset id for this run
            dataset = run_id_dataset_id_mapping[run.run_id]

            if not args.remove_submitted_runs:
                logger.info(run.format('Start submission for run {run_id} with dataset id {dataset_id}', 
                             dataset_id = dataset['dataset_id']))
            else:
                logger.info(run.format('Remove run {run_id} with dataset id {dataset_id}', 
                             dataset_id = dataset['dataset_id']))

            #iceprod.clean_run(dataset, run)

            if args.cleandatawarehouse:
                clean_datawarehouse(run, logger, args.dryrun, run_folder = run.format(config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER')))

            if args.remove_submitted_runs:
                logger.warning(run.format('Run {run_id} has been removed. Please note that no folders were removed nor the good run list has been modified.'))
                logger.warning(run.format('The IceProd DB has been modified ONLY!'))
                continue

            # Create output folder if not exists
            output = run.format(config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER'))
            if not os.path.exists(output):
                logger.debug('Create output folder: {0}'.format(output))

                if not args.dryrun:
                    os.makedirs(output)
                    # Set group to IceC-filt so IceProd2 jobs running as ice3simusr can write
                    os.chown(output,-1,5108)
                    os.chmod(output, 0o775)

            # Create GCD link
            gcd_file = run.get_gcd_file(exclude_run_folder_gcd = True) if args.special_gcd is None else File(args.special_gcd, logger)

            if gcd_file is None:
                logger.critical('No GCD found')
                counter.count('error')
                continue

            if not run.get_gcd_bad_dom_list_checked() or not run.get_gcd_generated():
                logger.critical('The GCD file has not been validated yet')
                counter.count('error')
                continue   

            if not args.nogcdlink:
                make_relative_symlink(gcd_file.path, run.format(config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER_GCD')), args.dryrun, logger, replace = True)
            else:
                logger.info('No GCD link will be created in run folder')

            # Put GCD symlink in run folder into cache since it is probably used in iceprod.submit_run()
            run.get_gcd_file(force_reload = True)

            # Check for input files
            input_files = list(run.get_pffilt_files())

            if not len(input_files):
                logger.critical('No input files found')
                counter.count('error')
                continue

            logger.info('Check files for DST header in good time range')
            # Check for short last files:
            # Start from the last file until we find a DST header within the good time range. Aggregate all files that were out of time range/do not have a dst header
            short_last_files = 0
            for i in range(len(input_files)):
                short_file = not has_subrun_dstheader_within_good_time_range(input_files[-(i + 1)], logger)

                logger.debug('{0} in good time range = {1}'.format(input_files[-(i + 1)].get_name(), not short_file))

                if not short_file:
                    # We're in good time range!
                    break

                short_last_files += short_file

            if short_last_files:
                logger.warning('No I3DSTHeader in last file(s) within good time range. Aggregate last {} files.'.format(short_last_files))
                run.set_last_files_aggregated(short_last_files)

            # Same for the fist files (e.g. if the good start time has been set)
            short_first_files = 0
            for i in range(len(input_files)):
                short_file = not has_subrun_dstheader_within_good_time_range(input_files[i], logger)

                logger.debug('{0} in good time range = {1}'.format(input_files[i].get_name(), not short_file))

                if not short_file:
                    # We're in good time range!
                    break

                short_first_files += short_file

            if short_first_files:
                logger.warning('No I3DSTHeader in first file(s) within good time range. Aggregate first {} files.'.format(short_first_files))
                run.set_first_files_aggregated(short_first_files)

            # Submit run
            iceprod.submit_run(dataset, run, checksumcache, 'PFFilt', aggregate_only_first_files = short_first_files, aggregate_only_last_files = short_last_files, gcd_file = args.special_gcd)

            # Write metadata
            if not args.nometadata:
                meta_file_dest = ''

                if args.dryrun:
                    meta_file_dest = get_tmpdir()
                else:
                    meta_file_dest = run.format(config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER'))

                metafile = MetaXMLFile(meta_file_dest, run, 'L2', dataset_id, logger)
                metafile.add_main_processing_info()
            else:
                logger.info("No meta data files will be written")

            logger.info('Run {0} submitted'.format(run.run_id))

            counter.count('submitted')
        except Exception as e:
            counter.count('error')
            logger.exception(e)

    logger.info('Run submission complete: {0}'.format(counter.get_summary()))
    return counter

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)

    parser.add_argument("-u","--username",type=str, action="store", default=None, dest="username", help="username")
    parser.add_argument("-g","--group",type=str, action="store", default="users", dest="group", help="group")
    parser.add_argument("--dataset-id", type = int, required = False, default = None, help="The dataset id. The default value is `None`. In this case it gets the dataset id from the database.")
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start submitting from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End submitting at this run")
    parser.add_argument("--runs", type = int, nargs = '*', required = False, help = "Submitting specific runs. Can be mixed with -s and -e")
    parser.add_argument("--cleandatawarehouse", action = "store_true", default = False, help = "Clean output files in datawarehouse as part of (re)submission process.")
    parser.add_argument("--resubmission", action = "store_true", default = False, help = "Resubmit the runs. Note that all runs are resubmitted. If a run would be submitted for the very first time, an error is thrown.")
    parser.add_argument("--nometadata", action = "store_true", default = False, help="Do not write meta data files")
    parser.add_argument("--nogcdlink", action = "store_true", default = False, help="Do not create a GCD link in the run folder")
    parser.add_argument("--remove-submitted-runs", action = "store_true", default = False, help="Instead of submitting runs, it will remove all runs that are specified from the iceprod DB. This means that those runs will be stopped to be processed.")
    parser.add_argument("--special-gcd", type = str, required = False, default = None, help = "Use a special GCD file, not the GCD file for this run. Note: This GCD file will be used for ALL runs that are submitted.")
    parser.add_argument("--cron", action = "store_true", default = False, help = "Use this option if you call this script via a cron")
    args = parser.parse_args()

    logfile = os.path.join(get_logdir(sublogpath = 'MainProcessing'), 'RunSubmission_')

    if args.cron:
        logfile += 'CRON_'

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

    if not len(runs) and args.cron:
        # OK, it's a cron and no runs have been specified explicitely. Let's try to submit all for the current season
        logger.info('No runs have been specified. Try to submit all runs of season {} that have were not submitted yet.'.format(config.get('DEFAULT', 'Season')))

        from libs.runs import get_all_runs_of_season
        runs = get_all_runs_of_season(config.get('DEFAULT', 'Season'), logger)
        logger.debug('Found {0} runs in the current season.'.format(len(runs), runs))

    if not len(runs):
        logger.critical("No runs given.")
        exit(1)

    if args.dataset_id is None:
        logger.info("No dataset id is specified. For each run the database will be checked to determine the L2 dataset id. If there are more than one dataset id for a specific run, the script will fail.")

    # Check if --cron option is enabled. If so, check if cron usage allowed by config
    lock = None
    if args.cron:
        if not config.getboolean('Level2', 'CronMainProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = Lock(os.path.basename(__file__), logger)
        lock.lock()

    counter = main(args, runs, logger)

    if args.cron:
        lock.unlock()
        cron_finished(os.path.basename(__file__), counter, logger, args.dryrun)
        lock = None # This triggers `__del__` and avoids: name 'open' is not defined

    logger.info('Done')
