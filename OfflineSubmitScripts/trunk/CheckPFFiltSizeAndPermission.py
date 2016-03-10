#!/usr/bin/env python

from __future__ import print_function
import glob
import os
import stat
import subprocess
from optparse import OptionParser
import SQLClient_dbs4 as dbs4

def GetOptions():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)

    parser.add_option("-s", "--startrun", type="int", default=0,
                                      dest="STARTRUN_", help="start submission from this run")

    parser.add_option("-e", "--endrun", type="int", default=0,
                                      dest="ENDRUN_", help="end submission at this run")
        
    (options,args) = parser.parse_args()
    if len(args) != 0:
        message = "Got undefined options:"
        for a in args:
            message += a
            message += " " 
        parser.error(message)

    params = {}
    params['startrun'] = options.STARTRUN_
    params['endrun'] = options.ENDRUN_

    return params

def CheckRun(runId, year, month, day, verbose = True):
	result = {'empty': [], 'permission': [], 'emptyAndPermission': []}

	if month < 10:
		month = '0' + str(month)

	if day < 10:
		day = '0' + str(day)

	path = '/data/exp/IceCube/' + str(year) + '/filtered/PFFilt/' + str(month) + str(day) + '/PFFilt_*' + str(runId) + '*.tar.bz2'

	if verbose:
		print('Check run ' + str(runId) + ': ', end = '')

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
                	print(str(len(result['empty'])) + ' empty files; ' + str(len(result['permission'])) + ' files with wrong permissions; ' + str(len(result['emptyAndPermission'])) + ' empty files with wrong permissions;')
        	else:
                	print('everything is allright')

	return result

if __name__ == '__main__':
	dbs4_ = dbs4.MySQL()

	options = GetOptions()

	if options['startrun'] > options['endrun']:
		print('The end run id must be equal or bigger than the start run id.')
		exit(1)

	runs = dbs4_.fetchall("""SELECT run_id, tStart FROM run_info_summary WHERE run_id BETWEEN %s AND %s"""%(options['startrun'], options['endrun']))

	if len(runs) == 0:
		print('Runs not found.')
		exit(1) 

	paths = {}

	for run in runs:
		date = run[1]
		runId = run[0]

		month = str(date.month)
		day = str(date.day)

		if date.month < 10: month = '0' + month
		if date.day < 10: day = '0' + day

		paths[runId] = '/data/exp/IceCube/' + str(date.year) + '/filtered/PFFilt/' + month + day + '/PFFilt_*' + str(runId) + '*.tar.bz2'

	emptyFiles = []
	wrongPermissions = []
	emptyAndWPerm = []

	print('Attempt to check files for ' + str(len(paths)) + ' runs')

	for run in runs:
		runId = run[0]
		date = run[1]

		result = CheckRun(runId, date.year, date.month, date.day)
		emptyFiles += result['empty']
		wrongPermissions += result['permission']
		emptyAndWPerm += result['emptyAndPermission']

	print('')
	print('---------------------------------------------')
	print('                  REPORT:')
	print('---------------------------------------------')
	print('---------------------------------------------')
	print('EMPTY FILES W/ READING PERMISSION (' + str(len(emptyFiles)) + ')')
	print('---------------------------------------------')
	for file in emptyFiles:
		print(file)

	print('')
        print('---------------------------------------------')
        print('EMPTY FILES W/O READING PERMISSION (' + str(len(emptyAndWPerm)) + ')')
        print('---------------------------------------------')
        for file in emptyAndWPerm:
                print(file)

	print('')
	print('---------------------------------------------')
	print('NOT EMPTY FILES W/O READING PERMISSION (' + str(len(wrongPermissions)) + ')')
	print('---------------------------------------------')
	for file in wrongPermissions:
		print(file)
