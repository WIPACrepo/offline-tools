#!/usr/bin/env python

"""
Functions to perform checks.
"""

import glob
import os
import stat


def pffilt_size_and_permission(runId, year, month, day, logger, verbose = True):
    """
    Checks if the PFFilt files of the given run has a proper file (size > 0) and proper file permission.

    Args:
        runId (int): The Run Id
        year (int): Year of the run
        month (int): Month of the run
        day (int): Day of the run
        logger (logging.Logger): The logger
        verbose (bool): Be more verbose? Default is `True`

    Returns:
        dict: Returns a dictionary with three lists of files: empty files, files with wrong permissions,
              and files that are empty and have wrong permissions:
              `{'empty': [], 'permission': [], 'emptyAndPermission': []}`
    """

    result = {'empty': [], 'permission': [], 'emptyAndPermission': []}

    if month < 10:
        month = '0' + str(month)

    if day < 10:
        day = '0' + str(day)

    path = '/data/exp/IceCube/' + str(year) + '/filtered/PFFilt/' + str(month) + str(day) + '/PFFilt_*' + str(runId) + '*.tar.bz2'

    if verbose:
        logger.info('Check run ' + str(runId) + ': ')

    files = glob.glob(path)
    files.sort()

    for file in files:
        st = os.stat(file)
        size = st.st_size

        empty = False
        perm = False

        if size == 0:
            empty = True

        if not (st.st_mode & stat.S_IRGRP):
            perm = True

        if empty and perm:
            result['emptyAndPermission'].append(file)
        elif empty:
            result['empty'].append(file)
        elif perm:
            result['permission'].append(file)

    if verbose:
        if len(result['empty']) > 0 or len(result['permission']) > 0 or len(result['emptyAndPermission']) > 0:
            logger.info("  %s empty files; %s files with wrong permissions; %s empty files with wrong permissions;"%(str(len(result['empty'])),
                                                                                                                  str(len(result['permission'])),
                                                                                                                  str(len(result['emptyAndPermission']))))
        else:
            logger.info('  everything is allright')

    return result

def has_sps_gcd_file(runId, year, month, day, logger):
    """
    Checks if the SPS GCD file exists for this run.

    Args:
        runId (int): The Run Id
        year (int): Year of the run
        month (int): Month of the run
        day (int): Day of the run
        logger (logging.Logger): The logger

    Returns:
        bool: `True` if the SPS GCD file exists for this run. Otherwise, `False` is returned.
    """

    if month < 10:
        month = '0' + str(month)

    if day < 10:
        day = '0' + str(day)

    path = '/data/exp/IceCube/' + str(year) + '/internal-system/sps-gcd/' + str(month) + str(day) + '/SPS-GCD_Run*' + str(runId) + '*.i3.tar.gz';

    files = glob.glob(path)

    return len(files) > 0;
