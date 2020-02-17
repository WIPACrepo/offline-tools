#!/usr/bin/env python

import os

from libs.logger import get_logger, delete_log_file
from libs.argparser import get_defaultparser
from libs.files import clean_datawarehouse, MetaXMLFile
from libs.process import Lock
from libs.config import get_config
from libs.utils import Counter, DBChecksumCache
from libs.runs import Run, get_validated_runs, get_all_runs_of_season, LoadRunDataException
from libs.iceprod1 import IceProd1
from libs.l3processing import get_gcd_file, get_cosmicray_mc_gcd_file
from libs.path import make_symlink, get_logdir, get_tmpdir
from libs.cron import cron_finished

from collections import OrderedDict

def main(run_ids, config, args, logger):
    counter = Counter(['handled', 'submitted', 'resubmitted', 'skipped', 'error'])
    iceprod = IceProd1(logger, args.dryrun)
    checksumcache = DBChecksumCache(logger, dryrun = args.dryrun)

    # Contains the get_dataset_info() + L3 info about the outdir
    dataset_info = config.get_level3_info()[args.destination_dataset_id]

    if dataset_info['pass'] > 1:
        logger.info('*** It is a pass {} dataset! ***'.format(dataset_info['pass']))

    # Set aggregate if not overridden:
    if args.aggregate is None:
        args.aggregate = dataset_info['aggregate']

        # Double check the config
        if args.aggregate < 1 or args.aggregate > 200:
            logger.critical('Unreasonable `aggregate` config for this datase. value: {0}'.format(args.aggregate))
            exit(1)

    if args.source_dataset_id is not None:
        logger.info('Source dataset id has been explicitely set through the parameter!')
        source_dataset_ids = [args.source_dataset_id]
    else:
        source_dataset_ids = config.get_source_dataset_ids(args.destination_dataset_id)

    logger.info('Source dataset: {s}, destination dataset: {d}'.format(s = source_dataset_ids, d = args.destination_dataset_id))

    if not len(source_dataset_ids):
        logger.critical('Did not find source dataset id for dataset id {0}'.format(args.destination_dataset_id))
        exit(1)

    run_dataset_mapping = {}

    if not len(run_ids):
        # Get all validated runs of the source dataset id
        # Note: The order of the source dataset ids matters. The first dataset id has the highest priority.
        # That means, if a run appears twice, the run from the highest priority will be chosen.
        all_runs_of_season = get_all_runs_of_season(dataset_info['season'], logger)

        for sd_id in source_dataset_ids:
            if args.ignore_l2_validation:
                logger.warning('Ignore L2 validation flag')

                for r in all_runs_of_season:
                    if r not in run_dataset_mapping and iceprod.get_run_status(sd_id, Run(r, logger)) == 'OK':
                        run_dataset_mapping[r] = sd_id
            else:
                for r in get_validated_runs(sd_id, logger):
                    if r not in run_dataset_mapping:
                        run_dataset_mapping[r] = sd_id
    else:
        # Ok, runs are specified on script start
        for sd_id in source_dataset_ids:
            for r in run_ids:
                if r not in run_dataset_mapping and iceprod.get_run_status(sd_id, Run(r, logger)) == 'OK':
                    run_dataset_mapping[r] = sd_id

        if len(run_ids) != len(run_dataset_mapping):
            logger.warning('For some runs the source run has not been found: {0}'.format(list(set(run_ids) - set(run_dataset_mapping.keys()))))
            logger.warning('Usually this only applies to bad runs.')

    run_ids = run_dataset_mapping.keys()

    logger.debug('run_dataset_mapping = {0}'.format(run_dataset_mapping))
    logger.debug('run_ids = {0}'.format(run_ids))

    # Create Run objects and filter bad runs
    validated_runs = {sd_id: get_validated_runs(sd_id, logger) for sd_id in source_dataset_ids}
    runs = []
    for run_id in run_ids:
        if run_id not in validated_runs[run_dataset_mapping[run_id]]:
            logger.warning('L2 files of run {} have not been validated yet. Skip this run.'.format(run_id))
            counter.count('skipped')
            continue

        try:
            r = Run(run_id, logger, dryrun = args.dryrun)
            r.load()

            if r.is_good_run():
                if iceprod.is_run_submitted(args.destination_dataset_id, r):
                    if args.resubmission:
                        logger.info('Run {0} will be resubmitted'.format(r.run_id))
                        counter.count('resubmitted')
                        runs.append(r)
                    else:
                        logger.info('Skip run {0} since this run has already been submitted'.format(r.run_id))
                        counter.count('skipped')
                else:
                    runs.append(r)
        except LoadRunDataException:
            logger.warning('Skipping run {0} since there are no DB entries'.format(run_id))
            counter.count('skipped')

    logger.debug('runs = {0}'.format(runs))

    for run in runs:
        counter.count('handled')

        handle_run(args, dataset_info, iceprod, config, run, run_dataset_mapping[run.run_id], counter, checksumcache, logger)

    logger.info('Run submission complete: {0}'.format(counter.get_summary()))
    return counter

def handle_run(args, dataset_info, iceprod, config, run, source_dataset_id, counter, checksumcache, logger):
    logger.info(run.format('Start submission for run {run_id} with dataset id {destination_dataset_id} (source: {source_dataset_id})',
            source_dataset_id = source_dataset_id,
            destination_dataset_id = args.destination_dataset_id
        )
    )

    outdir = run.format(dataset_info['path'])
    logger.info('Output folder: {0}'.format(outdir))

    # Are there L2 files?
    sub_runs = run.get_sub_runs().values()
    l2files = run.get_level2_files()

    if not len(l2files):
        logger.error('No L2 files were found')
        counter.count('error')
        return

    if len(sub_runs) != len(l2files):
        # Ok, there are some mismatches of how many L2 _could_ be there and how many are actually are there
        # This does NOT indicate an error. It could be an error. If sub runs have been marked as bad, there are no L2 files.
        # Therefore, check if this is the case

        logger.debug('L2 files: {0}'.format(len(l2files)))
        for f in l2files:
            logger.debug('  {0}'.format(f))

        logger.debug('Subruns: {0}'.format(len(sub_runs)))
        for f in sub_runs:
            logger.debug('  {0}'.format(f))

        sub_runs = [sr for sr in sub_runs if not sr.is_bad()]

        # Now should those lists be the same size
        if len(sub_runs) != len(l2files):
            # Now we have a problem
            logger.error('There is a mismatch between expected and actually present L2 files')

            present_subruns = [sr.sub_run_id for sr in l2files]
            expected_subruns = [sr.sub_run_id for sr in sub_runs]

            missing = [sr for sr in sub_runs if sr.sub_run_id not in present_subruns]
            unexpected = [sr for sr in l2files if sr.sub_run_id not in expected_subruns]

            if len(missing):
                logger.error('Missing L2 files: {0}'.format(missing))

            if len(unexpected):
                logger.error('Unexpected L2 files: {0}'.format(unexpected))

            counter.count('error')
            return

    # Find GCD file
    gcd_file = get_gcd_file(run, args, config, logger)
    eff_gcd_file = os.path.join(outdir, gcd_file.get_name())

    if gcd_file is None:
        logger.error('No GCD file found')
        counter.count('error')
        return

    logger.debug('GCD File: {0}'.format(gcd_file))

    special_files = []

    if args.cosmicray:
        # We also need the CosmicRay MC GCD file
        mc_gcd_file = get_cosmicray_mc_gcd_file(run.get_season(), config)
        eff_mc_gcd_file = os.path.join(outdir, mc_gcd_file.get_name())

        if mc_gcd_file is None:
            logger.error('CosmicRay WG: No MC GCD file was found.')
            counter.count('error')
            return
        elif not mc_gcd_file.exists():
            logger.error('CosmicRay WG: The MC GCD file does not exist: {0}'.format(mc_gcd_file))
            counter.count('error')
            return

        special_files.append(mc_gcd_file)

        logger.debug('CosmicRay WG MC GCD: {0}'.format(mc_gcd_file))

    # Ok, let's do something
    logger.info('Create run folder if not exists: {0}'.format(outdir))
    if not os.path.isdir(outdir) and not args.dryrun:
        os.makedirs(outdir)

    if args.cosmicray:
        logger.info('CosmicRay: Add MC GCD file to run folder')
        make_symlink(mc_gcd_file.path, eff_mc_gcd_file, args.dryrun, logger, replace = True)

    if not args.link_only_gcd and args.cleandatawarehouse:
        clean_datawarehouse(run, logger, args.dryrun, run_folder = outdir)

    logger.info('Add GCD file to run folder')
    make_symlink(gcd_file.path, eff_gcd_file, args.dryrun, logger, replace = True)

    iceprod.clean_run(args.destination_dataset_id, run)

    try:
        # Determine source data. First, we assume that we're going to process L3 data. So, the source is L2 data.
        # However, we also can process some higher level. Then we have a different data source that depends on the dataset id.
        ftype = 'Level2'

        if dataset_info['pass'] == 2:
            ftype = 'Level2pass2'

        if dataset_info['type'] != 'L3':
            ftype = ('LevelX', source_dataset_id)

        iceprod.submit_run(args.destination_dataset_id, run, checksumcache, ftype, aggregate = args.aggregate, gcd_file = gcd_file, special_files = special_files)

        # Write metadata
        if not args.nometadata:
            meta_file_dest = ''

            if args.dryrun:
                meta_file_dest = get_tmpdir()
            else:
                meta_file_dest = outdir

            metafile = MetaXMLFile(meta_file_dest, run, dataset_info['type'], args.destination_dataset_id, logger)
            metafile.add_main_processing_info()
        else:
            logger.info("No meta data files will be written")

        logger.info('Run {0} submitted'.format(run.run_id))
        counter.count('submitted')
    except Exception as e:
        counter.count('error')
        logger.exception(e)

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)
    parser.add_argument("--source-dataset-id", type = int, required = False, default = None, help="Dataset ID to read from, usually L2 dataset. Use this option only if you want override the configuration.")
    parser.add_argument("--destination-dataset-id", type = int, required = True, default = None, help="Dataset ID to write to, usually L3 dataset")
    parser.add_argument("--aggregate", type = int, default = None, help = "USE THIS OPTION IF YOU WANT TO OVERRIDE THE DATASET CONFIGURATION ONLY. DO USE THIS OPTION ONLY IF YOU KNOW WHAT YOU ARE DOING. Number of subruns to aggregate to form one job, needed when processing 1 subrun is really short.")
    parser.add_argument("--link-only-gcd", action = "store_true", default = False, help = "No jobs will be submitted but the GCD file(s) will be linked. Useful if some links are missing")
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start submitting from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End submitting at this run")
    parser.add_argument("--runs", type = int, nargs = '*', required = False, help = "Submitting specific runs. Can be mixed with -s and -e")
    parser.add_argument("--cleandatawarehouse", action = "store_true", default = False, help = "Clean output files in datawarehouse as part of (re)submission process.")
    parser.add_argument("--resubmission", action = "store_true", default = False, help = "Resubmit the runs. Note that all runs are resubmitted. If a run would be submitted for the very first time, an error is thrown.")
    parser.add_argument("--nometadata", action = "store_true", default = False, help="Do not write meta data files")
    parser.add_argument("--cron", action="store_true", default = False, help = "Execute as cron")
    parser.add_argument("--cosmicray", action = "store_true", default = False, help= " Important if you submit L3 jobs for the cosmic ray WG")
    parser.add_argument("--ignore-l2-validation", action = "store_true", default = False, help="If you do not care if L2 has not been validated yet. ONLY USE THIS OPTION IF YOU KNOW WHAT YOU ARE DOING! Not available with --cron")
    args = parser.parse_args()

    # Check if only GCDs should be linked
    # If yes, act like an dryrun except for the linking
    if args.link_only_gcd:
        args.dryrun = True

    # Log file
    logfile = os.path.join(get_logdir(sublogpath = 'L3Processing'), 'RunSubmission-' + str(args.destination_dataset_id) + '_')

    if args.cron:
        logfile += 'CRON_'

    if args.logfile is not None:
        logfile = args.logfile

    logger = get_logger(args.loglevel, logfile)

    config = get_config(logger)

    # Check arguments
    if args.aggregate is not None and (args.aggregate < 1 or args.aggregate > 200):
        logger.critical('Unreasonable --aggregate parameter value: {0}'.format(args.aggregate))
        exit(1)

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

    # Only allow empty runs if it is a cron job. If it is a cron job, runs will be chosen automatically
    if not len(runs) and not args.cron:
        logger.critical("No runs given.")
        exit(1)
    elif len(runs) and args.cron:
        logger.critical('Script has been executed as cron but has also specific runs passed. That\'s not allowed.')
        exit(1)

    # We need at least the destination dataset id
    if args.destination_dataset_id is None:
        logger.critical("--destination-dataset-id is required")
        exit(1)

    delete_log = False
    lock = None
    if args.cron:
        if not config.getboolean('Level3', 'CronMainSubmission'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = Lock(os.path.basename(__file__), logger)
        lock.lock()

        # If it is a cron, delete the log if nothing happened
        delete_log = True

    counter = main(runs, config, args, logger)

    if counter.get('handled') > 0:
        delete_log = False

    if lock is not None:
        lock.unlock()
        cron_finished(os.path.basename(__file__), counter, logger, args.dryrun)

    if delete_log:
        delete_log_file(logger)

