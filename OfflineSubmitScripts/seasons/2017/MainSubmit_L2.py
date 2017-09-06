#!/usr/bin/env python

import os

from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.iceprod1 import IceProd1
from libs.config import get_config
from libs.runs import Run, LoadRunDataException
from libs.files import clean_datawarehouse, MetaXMLFile, has_subrun_dstheader_within_good_time_range
from libs.path import make_relative_symlink, get_logdir, get_tmpdir
from libs.databaseconnection import DatabaseConnection
from libs.utils import Counter, DBChecksumCache

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

        run_id_dataset_id_mapping[run_id] = dataset_id

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

    iceprod = IceProd1(logger, args.dryrun)

    # Check if the resubmission flag has been set and if all runs are due for a resubmission
    if args.resubmission:
        for run in runs:
            if not iceprod.is_run_submitted(run_id_dataset_id_mapping[run.run_id], run):
                logger.critical('Run {0} has not been submitted before. Do not use the resubmission flag to submit runs for the first time.'.format(run.run_id))
                exit(1)

    checksumcache = DBChecksumCache(logger, dryrun = args.dryrun)

    for run in runs:
        counter.count('handled')

        try:
            # Dataset id for this run
            dataset_id = run_id_dataset_id_mapping[run.run_id]

            if not args.remove_submitted_runs:
                logger.info(run.format('Start submission for run {run_id} with dataset id {dataset_id}', dataset_id = dataset_id))
            else:
                logger.info(run.format('Remove run {run_id} with dataset id {dataset_id}', dataset_id = dataset_id))

            iceprod.clean_run(dataset_id, run)

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

            # Create GCD link
            gcd_file = run.get_gcd_file(exclude_run_folder_gcd = True)

            if gcd_file is None:
                logger.critical('No GCD found')
                counter.count('error')
                continue

            if not run.get_gcd_bad_dom_list_checked() or not run.get_gcd_generated():
                logger.critical('The GCD file has not been validated yet')
                counter.count('error')
                continue   

            make_relative_symlink(gcd_file.path, run.format(config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER_GCD')), args.dryrun, logger, replace = True)

            # Put GCD symlink in run folder into cache since it is probably used in iceprod.submit_run()
            run.get_gcd_file(force_reload = True)

            # Check for input files
            input_files = run.get_pffilt_files()

            if not len(input_files):
                logger.critical('No input files found')
                counter.count('error')
                continue

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
            iceprod.submit_run(dataset_id, run, checksumcache, 'PFFilt', aggregate_only_first_files = short_first_files, aggregate_only_last_files = short_last_files)

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

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)
    parser.add_argument("--dataset-id", type = int, required = False, default = None, help="The dataset id. The default value is `None`. In this case it gets the dataset id from the database.")
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start submitting from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End submitting at this run")
    parser.add_argument("--runs", type = int, nargs = '*', required = False, help = "Submitting specific runs. Can be mixed with -s and -e")
    parser.add_argument("--cleandatawarehouse", action = "store_true", default = False, help = "Clean output files in datawarehouse as part of (re)submission process.")
    parser.add_argument("--resubmission", action = "store_true", default = False, help = "Resubmit the runs. Note that all runs are resubmitted. If a run would be submitted for the very first time, an error is thrown.")
    parser.add_argument("--nometadata", action = "store_true", default = False, help="Do not write meta data files")
    parser.add_argument("--remove-submitted-runs", action = "store_true", default = False, help="Instead of submitting runs, it will remove all runs that are specified from the iceprod DB. This means that those runs will be stopped to be processed.")
    args = parser.parse_args()

    logfile = os.path.join(get_logdir(sublogpath = 'MainProcessing'), 'RunSubmission_')

    if args.logfile is not None:
        logfile = args.logfile

    logger = get_logger(args.loglevel, logfile)

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
        logger.critical("No runs given.")
        exit(1)

    if args.dataset_id is None:
        logger.info("No dataset id is specified. For each run the database will be checked to determine the L2 dataset id. If there are more than one dataset id for a specific run, the script will fail.")

    main(args, runs, logger)

    logger.info('Done')
