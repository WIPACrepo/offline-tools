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
	if month < 10:
		month = '0' + str(month)

	if day < 10:
		day = '0' + str(day)

	path = '/data/exp/IceCube/' + str(year) + '/internal-system/sps-gcd/' + str(month) + str(day) + '/SPS-GCD_Run*' + str(runId) + '*.i3.tar.gz';

	if verbose:
		print('Check run ' + str(runId) + ': ', end = '')

        files = glob.glob(path)

	return len(files) > 0;

if __name__ == '__main__':
	dbs4_ = dbs4.MySQL()

	options = GetOptions()

	if options['startrun'] > options['endrun']:
		print('The end run id must be equal or bigger than the start run id.')
		exit(1)

	runs = dbs4_.fetchall("""SELECT run_id, good_tStart FROM grl_snapshot_info WHERE run_id BETWEEN %s AND %s AND (good_i3 = 1 OR good_it = 1) AND PoleGCDCheck IS NULL"""%(options['startrun'], options['endrun']))

	if len(runs) == 0:
		print('Runs not found.')
		exit(1) 

	print('Attempt to check files for ' + str(len(runs)) + ' runs')

	count = 0
	for run in runs:
		runId = run[0]
		date = run[1]

		if CheckRun(runId, date.year, date.month, date.day):
			print("""Run %s has SPS-GCD file but hanot been checked by PoleGCDCheck"""%(runId))
		else:
			print("""No SPS-GCD file for run %s"""%(runId))
			count += 1
	print("""%s runs have missing SPS-GCD file"""%(count))
