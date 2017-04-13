
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

def validate_GCD(jobs, run, logger):
    """
    Performes a couple of checks for the GCD file.

    * GCD status in DB
    * Do al jobs use the same GCD file?
    * Do use all jobs the expected GCD file
    * Does the expected GCD file exist
    * Check if several GCD files exist in run folder
    * Check if the run has a verified GCD file

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

    gcd_name_pattern = config.get('Level2', 'RunFolderGCD')
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

    # Check if run has verified GCD file
    verified_gcd_path = run.format(config.get('GCD', 'VerifiedGCDPath'))
    if not os.path.exists(verified_gcd_path):
        logger.error('Did not find the verified GCD file for this run. Expected path is {0}'.format(verified_gcd_path))
        return False

    logger.info('Found verified GCD file for this run')

    # Yay! All checks passed
    return True

def validate_files(iceprod, dataset_id, run, logger):
    config = get_config(logger)
    jobs = iceprod.get_jobs(dataset_id, run)

    bad_sub_runs = [sr.sub_run_id for sr in run.get_sub_runs().values() if sr.is_bad()]

    # GCD checks
    if not validate_GCD(jobs, run, logger):
        return False

    # Check of all output files are existing
    # Also check checksums
    missing_files = []
    checksum_fails = []
    missing_l2_files = []
    l2_files = []

    for sub_run_id, job in jobs.items():
        if sub_run_id in bad_sub_runs:
            logger.info('Sub run {0} is marked as bad. Do not check it.'.format(sub_run_id))
            continue

        found_l2_output = False
        expected_l2_file_name = run.format(config.get('Level2', 'Level2File'), sub_run_id = sub_run_id)

        logger.info('Calculating checksums for sub run {0}'.format(sub_run_id))

        for f in job['output']:
            if f['path'] == expected_l2_file_name:
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

        if not found_l2_output:
            missing_l2_files.append(expected_l2_file_name)

    if len(missing_files):
        logger.error('{0} missing output files:'.format(len(missing_files)))
        for f in missing_files:
            logger.error('Path {0}'.format(f))

    if len(checksum_fails):
        logger.error('{0} checksum mismatches:'.format(len(checksum_fails)))
        for e in checksum_fails:
            logger.error('Path {path}: current = {current_checksum}, iceprod = {iceprod_checksum}'.format(e))

    if len(missing_l2_files):
        logger.error('{0} missing L2 files:'.format(len(missing_l2_files)))
        for f in missing_l2_files:
            logger.error('Path {0}'.format(f))

    if len(missing_files) or len(checksum_fails) or len(missing_l2_files):
        return False

    logger.info('All output files exists and have the currect checksums and all expected L2 files exists')

    # Check number of processed L2 files and available L2 files
    l2_files_on_disk = run.get_level2_files()
    if len(l2_files) != len(l2_files_on_disk):
        # Since we checked if all output files exist, the only case can be that we have more
        # files on disk than processed. That would be odd
        logger.error('Found more L2 files than actually processed:')
        odd_files = list(set([f.path for f in l2_files_on_disk]) - set(l2_files))
        for f in odd_files:
            logger.error('Path {0}'.format(f))

        return False

    logger.info('No unexpected L2 files found')

    # All checks passed
    return True

