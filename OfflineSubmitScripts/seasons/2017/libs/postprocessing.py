
import os

from glob import glob
from stringmanipulation import replace_var, replace_all_vars
from config import get_config
from files import File

def _get_checksum(f):
    """
    Returns the type and the value of the checksum.

    Only for internal use for the postprocessing module.

    Args:
        f (dict): Value of IceProdInterface.get_jobs['input'][*]

    Return:
        tuple: (type, checksum)
    """

    checksumtype = ['md5', 'sha512']

    for t in checksumtype:
        if t in f:
            return (t, f[t])

    raise Exception('Did not find checksum')

def check_for_stream_errors(f, logger):
    """
    Loops over all frames of the file in order to see if there is a stream error.

    Args:
        f (str|files.File): The file
        logger (Logger): The logger

    Returns:
        bool: `True` if the files has no stream errors, `False` if it has.
    """

    try:
        file_name = f.path
    except AttributeError:
        file_name = f

    from icecube import dataio, dataclasses
    i3file = dataio.I3File(file_name)

    logger.debug('file_name = {}'.format(file_name))

    try:
        while i3file.more():
            frame = i3file.pop_frame()
        return True
    except RuntimeError as e:
        if 'ZSTD_decompressStream' in e:
            return False
        else:
            raise e
    finally:
        i3file.close()

    # We should never reach this point
    raise RuntimeError('Unexpected behaviour')

def validate_GCD_L2(jobs, run, logger):
    """
    Performes a couple of checks for the GCD file.

    * GCD status in DB
    * Do al jobs use the same GCD file?
    * Do use all jobs the expected GCD file
    * Does the expected GCD file exist
    * Check if several GCD files exist in run folder

    Args:
        jobs (dict): Output of IceProdInterface.get_jobs()
        run (runs.Run): The run
        logger (Logger): The logger

    Returns:
        boolean: `True` if all checks have been passed. If `False`, an error has been logged for more details.  
    """

    config = get_config(logger)

    # Check GCD file status
    if not run.get_gcd_generated() or not run.get_gcd_bad_dom_list_checked():
        logger.error('GCD file has not been validated yet')
        return False

    logger.info('GCD file has been validated')

    gcd_name_pattern = config.get_l2_path_pattern(run.get_season(), 'RUN_FOLDER_GCD')
    gcd_name = run.format(gcd_name_pattern)

    # Check if all jobs have the same GCD file
    # Assuming that the letters `GCD` is in the GCD file name
    gcd_file_names = set()
    gcd_file_checksums = set()
    for job in jobs.values():
        for f in job['input']:
            if 'gcd' in os.path.basename(f['path'].lower()):
                gcd_file_names.add(f['path'])
                gcd_file_checksums.add(_get_checksum(f)[1])

    if len(gcd_file_names) > 1:
        logger.error('Found more than one GCD file: {0}'.format(gcd_file_names))
        return False

    logger.info('Not more than one GCD file has been used for this run')

    if len(gcd_file_checksums) > 1:
        logger.error('Did find more than one checksums for GCD files: {0}'.format(gcd_file_checksums))
        return False

    logger.info('Not more than one checksum for GCD files')

    if not len(gcd_file_names):
        logger.warning('Did not find any input file that has `GCD` in the file name')

    if not len(gcd_file_checksums):
        logger.warning('Did not find any checksum for GCD file with `GCD` in file name')

    # Check if GCD file is on run folder
    if len(gcd_file_names):
        gcd_file_names = list(gcd_file_names)

        if not os.path.exists(gcd_file_names[0]):
            logger.error('Input GCD file {0} does not exist'.format(gcd_file_names[0]))
            return False
        else:
            logger.info('GCD file is in run folder')

    # Check if the input GCD file name fits the expectation:
    # Each job needs the GCD file. Also check once if the GCD file exists in run folder

    gcd_file_checksums = set()

    for sub_run, job in jobs.items():
        found = False
        for f in job['input']:
            if f['path'] == gcd_name:
                found = True
                gcd_file_checksums.add(_get_checksum(f))
                break

        if not found:
            logger.error('Job (job_id: {job_id}) for sub run {sub_run} has no GCD file named {gcd}'.format(job_id = job['job_id'], sub_run = sub_run, gcd = gcd_name))
            return False

    logger.info('All jobs used the expected GCD file')

    if not os.path.exists(gcd_name):
        logger.error('GCD file {gcd} does not exist'.format(gcd = gcd_name))
        return False

    logger.info('The GCD file is present in the datawarehouse')

    if len(gcd_file_checksums) != 1:
        logger.error('Did not find exactly one checksum for GCD file.')
        return False

    logger.info('Found exactly one checksum for GCD file')

    gcd_file_checksums = list(gcd_file_checksums)

    # Check checksum
    gcd_checksum = File.get_checksum(gcd_name, gcd_file_checksums[0][0], logger)
    if gcd_checksum != gcd_file_checksums[0][1]:
        logger.error('GCD file checksums test failed: present = {0}, iceprod = {1}'.format(gcd_checksum, gcd_file_checksums[0][1]))
        return False

    logger.info('GCD checksum is correct')

    # Check if there are several GCD files in run folder
    # Ok, since we know the run folder exactly, use the folder that has all variables replaced like {year}, {month} etc.
    # However, the file name in the folder should not have filled the variables since we don't know how they could
    # have been filled.
    gcd_name_glob = os.path.join(os.path.dirname(gcd_name), os.path.basename(gcd_name_pattern))
    gcd_name_glob = replace_all_vars(gcd_name_glob, '*')

    files = glob(gcd_name_glob)
    if not len(files):
        logger.error('Did not find any GCD file in run folder. That is odd since we actually checked this right before this check...')
        return False
    elif len(files) > 1:
        logger.error('Found more than one GCD file in run folder. That is confusing. Resolve this problem by deleting the wrong file. The currect file should be {0}'.format(gcd_name))
        return False

    logger.info('Found exactly one GCD file in run folder')

    # Yay! All checks passed
    return True

def validate_L3_files(jobs, run, dataset_id, logger):
    # Check if run folder is as expected
    l3 = get_config(logger).get_level3_info()
    if int(dataset_id) not in l3:
        logger.error('Did not find L3 configuration in DB')
        return False

    l3info = l3[int(dataset_id)]
    expected_path = os.path.join(run.format(l3info['path']), '')
    used_path = list(set([os.path.join(os.path.dirname(e['path']), '') for e in jobs[jobs.keys()[0]]['output']]))
   
    logger.debug('used_path = {}'.format(used_path))
 
    if len(used_path) > 1:
        logger.error('More than one output folder: {}'.format(used_path))
        return False

    if not len(used_path):
        logger.error('No output folder. How can this happen?')
        return False

    used_path = used_path[0]
    if used_path != expected_path:
        logger.error('The actual output folder does not match the expected output folder:')
        logger.error('Expected: {}'.format(expected_path))
        logger.error('Used:     {}'.format(used_path))
        return False

    # Check if GCD link in run folder is available
    gcds_in_folder = [f for f in glob(os.path.join(expected_path, '*')) if 'gcd' in f.lower()]

    if not len(gcds_in_folder):
        logger.error('No GCD link found in {}'.format(expected_path))
        return False

    if len(gcds_in_folder) > 1:
        logger.error('More than one GCD link found in {}'.format(expected_path))
        return False

    gcd_in_folder = gcds_in_folder[0]

    # Check if the linked file is the same as used in processing
    used_gcd = list(set([e['path'] for e in jobs[jobs.keys()[0]]['input'] if 'gcd' in e['path'].lower()]))
    used_gcd = used_gcd[0]

    gcd_link_checksum = File.get_sha512(gcd_in_folder, logger)
    gcd_job_checksum = File.get_sha512(used_gcd, logger)

    if gcd_link_checksum != gcd_job_checksum:
        logger.error('The used and linked GCD files are not the same!')
        return False

    logger.info('Passed L3 GCD checks')
    return True

def validate_files(iceprod, dataset_id, run, checksumcache, logger, level = 'L2'):
    """
    Validates the files of a run including the GCD file. Works for L2 and L3 production.

    Args:
        iceprod (iceprodinterface.IceProdInterface): The IceProd interface
        dataset_id (int): The dataset id
        run (runs.Run): The run
        checksumcache (utils.ChecksumCache): The checksum cache
        logger (Logger): The logger
        level (str): `L2` or `L3`.
    """

    if level not in ('L2', 'L3', 'L4'):
        logger.critical('Invalid value for `level`: {}'.format(level))
        exit(1)

    config = get_config(logger)

    dataset_info = config.get_level3_info()[int(dataset_id)]

    jobs = iceprod.get_jobs(dataset_id, run)

    bad_sub_runs = [sr.sub_run_id for sr in run.get_sub_runs().values() if sr.is_bad()]

    # GCD checks
    if level == 'L2':
        if not validate_GCD_L2(jobs, run, logger):
            return False
    else:
        if not validate_L3_files(jobs, run, dataset_id, logger):
            return False

    # Check of all output files are existing
    # Also check checksums
    missing_files = []
    checksum_fails = []
    missing_lx_files = []
    stream_errors = []
    l2_files = []
    in_files = []

    for sub_run_id, job in jobs.items():
        if sub_run_id in bad_sub_runs:
            logger.info('Sub run {0} is marked as bad. Do not check it.'.format(sub_run_id))
            continue

        found_l2_output = False

        if level == 'L2':
            expected_lx_file_name = run.format(config.get_l2_path_pattern(run.get_season(), 'DATA'), sub_run_id = sub_run_id)
        elif level == 'L3':
            expected_lx_file_name = run.format(os.path.join(dataset_info['path'], config.get('Level3', 'FileName')), sub_run_id = sub_run_id)
        elif level == 'L4':
            expected_lx_file_name = run.format(os.path.join(dataset_info['path'], config.get('Level4', 'FileName')), sub_run_id = sub_run_id)
        else:
            raise RuntimeError('Unknown level: {}'.format(level))

        logger.info('Calculating checksums and looking for stream errors for sub run {0}'.format(sub_run_id))

        for f in job['input']:
            in_files.append(f['path'])

        for f in job['output']:
            if f['path'] == expected_lx_file_name:
                found_l2_output = True
                l2_files.append(f['path'])

            if not os.path.exists(f['path']):
                missing_files.append(f['path'])
            else:
                # Check checksum
                ctype, checksum = _get_checksum(f)
                current_checksum = File.get_checksum(f['path'], ctype, logger)

                if current_checksum != checksum:
                    checksum_fails.append({'path': f['path'], 'current_checksum': current_checksum, 'iceprod_checksum': checksum})
                else:
                    cache = True

                    # Let's check for stream errors
                    if '.i3' in os.path.basename(f['path']):
                        if not check_for_stream_errors(f['path'], logger):
                            stream_errors.append(f['path'])
                            cache = False

                    if cache:
                        # OK, let's cache this checksum
                        checksumcache.set_checksum(f['path'], ctype, current_checksum)

        if not found_l2_output:
            missing_lx_files.append(expected_lx_file_name)

    if len(missing_files):
        logger.error('{0} missing output files:'.format(len(missing_files)))
        for f in missing_files:
            logger.error('Path {0}'.format(f))

    if len(checksum_fails):
        logger.error('{0} checksum mismatches:'.format(len(checksum_fails)))
        for e in checksum_fails:
            logger.error('Path {path}: current = {current_checksum}, iceprod = {iceprod_checksum}'.format(**e))

    if len(missing_lx_files):
        logger.error('{0} missing files:'.format(len(missing_lx_files)))
        for f in missing_lx_files:
            logger.error('Path {0}'.format(f))

    if len(stream_errors):
        logger.error('{0} stream errors found:'.format(len(stream_errors)))
        for f in stream_errors:
            logger.error('Path {}'.format(f))

    if len(missing_files) or len(checksum_fails) or len(missing_lx_files) or len(stream_errors):
        return False

    logger.info('All output files exists and have the currect checksums and all expected {} files exists'.format(level))

    # Check number of processed LX files and available LX files
    lx_files_on_disk = run.get_level2_files() if level == 'L2' else run.get_levelx_files(dataset_id)# get_level3_files(run, dataset_id, logger)
    if len(l2_files) != len(lx_files_on_disk):
        logger.info('l2_files = {}'.format(l2_files))
        logger.info('lx_files_on_disk = {}'.format(lx_files_on_disk))

        # Since we checked if all output files exist, the only case can be that we have more
        # files on disk than processed. That would be odd
        logger.error('Found more {} files than actually processed:'.format(level))
        odd_files = list(set([f.path for f in lx_files_on_disk]) - set(l2_files))
        for f in odd_files:
            logger.error('Path {0}'.format(f))

        return False

    logger.info('No unexpected {} files found'.format(level))

    # Check if all L2 files were processed
    if level in ('L3', 'L4'):
        lx_in_files = set([f for f in in_files if 'gcd' not in f.lower()])

        if level == 'L3':
            lxm1_files_on_disk = set([f.path for f in run.get_level2_files()])
        else:
            source_datasets = config.get_source_dataset_ids(dataset_id)

            for sds in source_datasets:
                lxm1_files_on_disk = set([f.path for f in run.get_levelx_files(sds)])

                if len(lxm1_files_on_disk):
                    break

        if lx_in_files == lxm1_files_on_disk:
            logger.info('All input files have been processed')
        else:
            not_processed = lxm1_files_on_disk - lx_in_files

            if len(not_processed):
                logger.error('Some input files have not been processed:')

                for i, f in enumerate(list(not_processed)):
                    logger.error('  {0}: {1}'.format(i, f))

            unknown_input_files = lx_in_files - lxm1_files_on_disk
            if len(unknown_input_files):
                logger.error('L2/X files have been used as input files that are not present on disk:')

                for i, f in enumerate(list(unknown_input_files)):
                    logger.error('  {0}: {1}'.format(i, f))

            return False

    # All checks passed
    return True

