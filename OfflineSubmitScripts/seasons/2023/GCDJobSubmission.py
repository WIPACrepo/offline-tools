#!/usr/bin/env python

import os
import subprocess

from libs.config import get_config
from libs.argparser import get_defaultparser
from libs.logger import get_logger
from libs.path import get_logdir, get_tmpdir, get_rootdir, get_env_python_path
from libs.runs import Run, LoadRunDataException
from libs.databaseconnection import DatabaseConnection
from libs.process import Lock
from libs.cron import cron_finished
from libs.utils import Counter

def get_runs_without_gcd_file(season, db, logger):
    """
    Note: This is not a accurate list! It _can_ contain runs that already have a GCD file (e.g. if there are two entries w/ different
    production versions/snapshot ids. However, this does not break things since later in the script the correction run_id/production_version
    combination is picked.
    We just need to ensure that no run that _could_ have a missing GCD file is filtered.

    This function just prevents from loading unnecessary data by using the Run-class.
    """

    from libs.runs import get_all_runs_of_season
    all_runs = get_all_runs_of_season(season, logger)

    sql = 'SELECT run_id FROM i3filter.runs WHERE run_id IN ({}) AND NOT gcd_generated AND NOT gcd_bad_doms_validated'.format(','.join(str(r) for r in all_runs))
    result = db.fetchall(sql)
    return [r['run_id'] for r in result]

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)
    parser.add_argument("-s", "--startrun", type = int, required = False, default = None, help = "Start generating GCD files from this run")
    parser.add_argument("-e", "--endrun", type = int, required = False, default= None, help = "End generating GCD files at this run")
    parser.add_argument("--runs", type = int, nargs = '*', required = False, help = "Generate GCD file of specific runs. Can be mixed with -s and -e")
    parser.add_argument("--resubmission", action = "store_true", default = False, help = "Regenerate GCD file even if already attempted")
    parser.add_argument("--local-execution", action = "store_true", default = False, help = "Do not submit a condor file. Generate a .sh file inseatd. It contains the run commands in order to generate the GCD files on your local machine.")
    parser.add_argument("--cron", action = "store_true", default = False, help = "Use this option if you call this script via a cron")
    args = parser.parse_args() 

    logfile = os.path.join(get_logdir(sublogpath = 'MainProcessing'), 'SubmitGCDJobs_')

    if args.cron:
        logfile += 'CRON_'

    if args.logfile is not None:
        logfile = args.logfile

    logger = get_logger(args.loglevel, logfile)

    # Config
    config = get_config(logger)

    # Counter
    counter = Counter(['handled', 'submitted', 'skipped', 'error'])

    # Check if --cron option is enabled. If so, check if cron usage allowed by config
    lock = None
    if args.cron:
        if not config.getboolean('GCD', 'CronGCDProcessing'):
            logger.critical('It is currently not allowed to execute this script as cron. Check config file.')
            exit(1)

        # Check if cron is already running
        lock = Lock(os.path.basename(__file__), logger)
        lock.lock()

    # DB connection
    db = DatabaseConnection.get_connection('filter-db', logger)
    if db is None:
        raise Exception('No database connection')

    # Temporary submit file
    condor_file_path = config.get('GCDGeneration', 'TmpCondorSubmitFile')
    logger.debug('Temporary condor submit file: {0}'.format(condor_file_path))

    # I3Build
    i3build = config.get('Level2', 'I3_BUILD')

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
        logger.info('No runs have been specified. Try to generate GCD files for all runs of season {} that have no GCD generated yet.'.format(config.get('DEFAULT', 'Season')))

        runs = get_runs_without_gcd_file(config.get('DEFAULT', 'Season'), db, logger)
        logger.debug('Found {0} runs w/o GCD file: {1}'.format(len(runs), runs))

    if not len(runs):
        logger.critical("No runs given.")
        exit(1)

    logger.info("Generate GCD files for {0}".format(', '.join([str(r) for r in runs])))

    runs = [Run(run_id, logger = logger, dryrun = args.dryrun) for run_id in runs]

    if args.local_execution:
        from datetime import datetime
        bash_file = open(config.get('GCD', 'LocalExecutionBashFile'), 'w')
        bash_file.write('# This file needs to be execute manually!\n')
        bash_file.write('# This file was create at {0}\n'.format(datetime.now()))

    for run in runs:
        logger.info('Submit GCD generation script for run {0}'.format(run.run_id))

        counter.count('handled')

        try:
            if not run.is_good_run() and not run.is_test_run():
                logger.info('Skip run since it is a bad run and not a 24h test run.')
                counter.count('skipped')
                continue
        except LoadRunDataException as e:
            logger.error(str(e))
            counter.count('error')
            continue

        if run.get_gcd_file() is not None and not run.get_gcd_generated():
            logger.error('Some state mismatch: A GCD file exists but the DB says it has not been generated yet.')
            counter.count('error')
            continue

        if run.get_gcd_file() is None and run.get_gcd_generated():
            logger.error('Some state mismatch: A GCD file does not exist but the DB says it has been generated.')
            counter.count('error')
            continue

        if not args.resubmission and run.get_gcd_file() is not None:
            logger.info('Skip this run because it already has a GCD file. If you want to re-create it, use the --resubmission option.')
            counter.count('skipped')
            continue

        condor_log = run.format(config.get('GCDGeneration', 'CondorLog'))
        condor_err = run.format(config.get('GCDGeneration', 'CondorErrorLog'))
        out_log = run.format(config.get('GCDGeneration', 'OutLog'))

        logger.debug('condor_log = {0}'.format(condor_log))
        logger.debug('condor_err = {0}'.format(condor_err))
        logger.debug('out_log = {0}'.format(out_log))

        if not args.dryrun:
            if not os.path.exists(os.path.dirname(condor_log)):
                logger.info('{0} does not exist yet. Creating...'.format(os.path.dirname(condor_log)))
                os.makedirs(os.path.dirname(condor_log))
            if not os.path.exists(os.path.dirname(condor_err)):
                logger.info('{0} does not exist yet. Creating...'.format(os.path.dirname(condor_err)))
                os.makedirs(os.path.dirname(condor_err))
            if not os.path.exists(os.path.dirname(out_log)):
                logger.info('{0} does not exist yet. Creating...'.format(os.path.dirname(out_log)))
                os.makedirs(os.path.dirname(out_log))

        if args.local_execution:
            bash_file.write(run.format('nohup {python} {script} --run-id {run_id} --production-version {production_version} --snapshot-id {snapshot_id} > {logfile} &\n', python = get_env_python_path(), script = os.path.join(get_rootdir(), 'GCDGenerator.py'), logfile = run.format(config.get('GCD', 'LocalExecutionLogFile'))))
        else:
            with open(condor_file_path,"w") as condor_file:
                condor_file.write("Universe = vanilla ")
                condor_file.write("\nrequest_memory = 4000")
                condor_file.write('\nExecutable = {0}'.format(get_env_python_path()))
                condor_file.write(run.format("\narguments =  {script} --run-id {run_id} --production-version {production_version} --snapshot-id {snapshot_id} --loglevel 10", script = os.path.join(get_rootdir(), 'GCDGenerator.py')))
                condor_file.write("\nLog = {0}".format(condor_log))
                condor_file.write("\nError = {0}".format(condor_err))
                condor_file.write("\nOutput = {0}".format(out_log))
                condor_file.write("\nNotification = Never")
                condor_file.write("\npriority = 15")
                condor_file.write("\nQueue")

        if not args.dryrun:
            if args.resubmission:
                logger.debug("Update i3filter.runs: gcd_generated = 0, gcd_bad_doms_validated = 0, gcd_pole_validation = NULL, gcd_template_validation = NULL")
                db.execute(run.format("""UPDATE i3filter.runs
                                 SET gcd_generated = 0, gcd_bad_doms_validated = 0, gcd_pole_validation = NULL, gcd_template_validation = NULL
                                 WHERE run_id = {run_id}
                                    AND snapshot_id = {snapshot_id}
                                    AND production_version = {production_version}
                              """))

            if not args.local_execution:
                logger.debug("Execute `condor_submit {0}`".format(condor_file_path))
                processoutput = subprocess.check_output("condor_submit {0}".format(condor_file_path), shell = True, stderr = subprocess.STDOUT)
                logger.info(processoutput.strip())
                counter.count('submitted')

    if args.local_execution:
        bash_file.close()

        logger.warning('--local-execution option was enabled. NO JOBS HAVE BEEN SUBMITTED. NO GCD GENERATION PROCESSES ARE RUNNING CURRENTLY!')
        logger.warning('You need to execute {0} manually on that machine you would like!'.format(config.get('GCD', 'LocalExecutionBashFile')))

    if args.cron:
        lock.unlock()
        cron_finished(os.path.basename(__file__), counter, logger, args.dryrun)

    logger.info('Done')

