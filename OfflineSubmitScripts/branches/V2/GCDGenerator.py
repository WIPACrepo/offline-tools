#!/usr/bin/env python

import os

from icecube import icetray

from libs.argparser import get_defaultparser
from libs.logger import DummyLogger, get_logger
from libs.path import get_condor_scratch_folder, make_relative_symlink
from libs.config import get_config
from libs.runs import Run
from libs.gcdgeneration import *
from libs.databaseconnection import DatabaseConnection

def main(run_id, production_version, snapshot_id, outdir, logger):
    scratch_folder = get_condor_scratch_folder(default = './')
    config = get_config(logger)
    db = DatabaseConnection.get_connection('filter-db', logger)
    spe_correction_file = config.get('GCDGeneration', 'SpeCorrectionFile')

    logger.debug('Scratch folder: {0}'.format(scratch_folder))

    tmp_out_file = "tmp_L2_{run_id}.out".format(run_id = run_id)
    tmp_out_file = os.path.join(scratch_folder, tmp_out_file)
    icetray.logging.rotating_files(tmp_out_file)

    # Run information
    run = Run(run_id, logger)

    # Format paths
    gcd_data_path = run.format(config.get('GCD', 'GCDDataPath'))
    gcd_all_path = run.format(config.get('GCD', 'AllGCDPath'))
    gcd_verified_path = run.format(config.get('GCD', 'VerifiedGCDPath'))

    # If outdir was set, use new location
    if outdir:
        gcd_data_path = os.path.join(outdir, os.path.basename(gcd_data_path))
    else:
        if not os.path.exists(os.path.dirname(gcd_data_path)):
            os.mkdir(os.path.dirname(gcd_data_path))

    # Create folders if neccessary
    if not os.path.exists(os.path.dirname(gcd_all_path)):
        logger.debug('Create {0}'.format(os.path.dirname(gcd_all_path)))
        os.mkdir(os.path.dirname(gcd_all_path))
    if not os.path.exists(os.path.dirname(gcd_verified_path)):
        logger.debug('Create {0}'.format(os.path.dirname(gcd_verified_path)))
        os.mkdir(os.path.dirname(gcd_verified_path))

    # Generate the actual GCD file
    generate_gcd(run, gcd_data_path, spe_correction_file, logger)

    logger.info(run.format("==== Completed GCD generation attempt for run {run_id}"))

    if os.path.isfile(gcd_data_path) and os.path.getsize(gcd_data_path) > 0:
        if not outdir:
            make_relative_symlink(gcd_data_path, gcd_all_path)

        logger.info(run.format("Auditing GCD file for run {run_id}"))

        # GCD audit
        run_gcd_audit(gcd_data_path, logger)
        if parse_gcd_audit_output(tmp_out_file, logger) and not outdir:
            db.execute(run.format("UPDATE i3filter.runs SET gcd_generated = 1 WHERE run_id = {run_id} and snapshot_id = {snapshot_id}"))

        logger.info("==== END: Auditing GCD file ====")

        # Bad doms audit
        logger.info(run.format("Start: BadDOMs Auditing for GCD file for run {run_id}"))

        infiles = sorted([s.path for s in run.get_pffilt_files()])

        # Try auditing first subrun
        tmp_file = os.path.join(scratch_folder, ('tmp_' + os.path.basename(infiles[0])).replace(".tar.bz2",".i3.bz2"))
        rehydrate(gcd_data_path, infiles[0], tmp_file, logger)

        logger.info("First infile for bad dom audit check: {0}".format(infiles[0]))
        run_bad_dom_audit(gcd_data_path, tmp_file, logger)
        first_bad_dom_audit_result = parse_bad_dom_audit(tmp_out_file, logger)

        os.remove(tmp_file)

        if first_bad_dom_audit_result:
            # This further check is necessary in case a DOM drops out during a run, this ensures that all DOMs
            # are present all through data taking
            
            # Pick 2nd to last sub-run if you have more than 2 sub-runs, otherwise just pick last sub-run.
            # Prefer to use 2nd to last sub-run because many last sub-runs are really short resulting in false alarm.
            # Any run with < 3 subruns is probably deemed bad because of the run length requirement
            if len(infiles) > 2:
                last_bd_autit_infile = infiles[-2]
            else:
                last_bd_autit_infile = infiles[-1]

            tmp_file = os.path.join(scratch_folder, ('tmp_' + os.path.basename(last_bd_autit_infile)).replace(".tar.bz2",".i3.bz2"))
            rehydrate(gcd_data_path, last_bd_autit_infile, tmp_file, logger)

            logger.info("Second/Last infile for bad dom audit check: {0}".format(last_bd_autit_infile))
            run_bad_dom_audit(gcd_data_path, tmp_file, logger)
            last_bad_dom_audit_result = parse_bad_dom_audit(tmp_out_file, logger)
            
            os.remove(tmp_file)
            os.remove(tmp_out_file)

            if last_bad_dom_audit_result:
                if not outdir:
                    make_relative_symlink(gcd_data_path, gcd_verified_path)
                    db.execute(run.format("UPDATE i3filter.runs SET gcd_bad_doms_validated = 1 WHERE run_id = {run_id} and snapshot_id = {snapshot_id}"))

                logger.info("Bad doms audit for {0} OK".format(gcd_data_path))
            else:
                logger.error("Bad doms audit for {0} has errors".format(gcd_data_path))

        logger.info("==== End: Bad doms auditing ====")
    logger.info("End of GCD generation")

if __name__ == '__main__':
    parser = get_defaultparser(__doc__, logfile = False)

    parser.add_argument("--out", type = str, default = None, help = "The directory in which the GCD file should be written. Overrides the value of the configuration file.")
    parser.add_argument('--run-id', dest = 'run_id', help = "Run number", type = int)
    parser.add_argument('--production-version', help = "Production version", type = int)
    parser.add_argument('--snapshot-id', help = "Snapshot Id", type = int)
    args = parser.parse_args()

    logger = DummyLogger()
    logger.set_level(args.loglevel)

    logger.info("Generating GCD file for run {run_id} with production version {production_version} and snapshot id {snapshot_id}".format(run_id = args.run_id, snapshot_id = args.snapshot_id, production_version = args.production_version))
 
    main(args.run_id, args.production_version, args.snapshot_id, args.out, logger)



