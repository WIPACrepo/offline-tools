
import os
import sys
sys.path.append('/data/user/i3filter/SQLServers_n_Clients')
import SQLClient_dbs4

import matplotlib
matplotlib.use('Agg') 
import pylab

import argparse

dbs4 = SQLClient_dbs4.MySQL()

parser = argparse.ArgumentParser()
parser.add_argument('--l2dataset', help = "The L2 dataset", type = int, required = True)
parser.add_argument('--l3dataset', help = "The L3 dataset", type = int, required = True)
parser.add_argument('--out', help = "Output folder. If not specified: ./", default = './', type = str)
parser.add_argument('--xmax', help = "Max bin number. Default: Max entry", type = int)
parser.add_argument('--hours', help = "Use hours instead of days", action='store_true', default = False)
args = parser.parse_args()

l2 = dbs4.fetchall("""SELECT run_id, MAX(UNIX_TIMESTAMP(status_changed)) AS `time` 
    FROM run r JOIN job j ON r.queue_id = j.queue_id AND r.dataset_id = j.dataset_id 
    WHERE r.dataset_id = %s AND status_changed IS NOT NULL
    GROUP BY run_id""" % args.l2dataset, UseDict = True)

l3_data = dbs4.fetchall("""SELECT run_id, MAX(UNIX_TIMESTAMP(status_changed)) AS `time` 
    FROM run r JOIN job j ON r.queue_id = j.queue_id AND r.dataset_id = j.dataset_id
    WHERE r.dataset_id = %s AND status_changed IS NOT NULL 
    GROUP BY run_id""" % args.l3dataset, UseDict = True)

wg_name = dbs4.fetchall("""SELECT 
    name, dataset_id
FROM
    i3filter.offline_dataset_season ds
        JOIN
    i3filter.offline_working_groups wg ON wg.wid = ds.working_group
WHERE dataset_id = %s""" % args.l3dataset, UseDict = True)

wg_name = wg_name[0]['name']

keys = list(set([r['run_id'] for r in l2]) & set([r['run_id'] for r in l3_data]))

sorted(keys)

l2_time = {}
for run in l2:
    l2_time[run['run_id']] = run['time']

l3_data_time = {}
for run in l3_data:
    l3_data_time[run['run_id']] = run['time']

entries = []

unit = 'Days'
divider = 3600 * 24

if args.hours:
    unit = 'Hours'
    divider = 3600

#print "# Run Id\tdelta T"
for key in keys:
#    if (l3_data_time[key] - l2_time[key]) / 3600 > 700:
#        print "%s\t%s" % (key, (l3_data_time[key] - l2_time[key]) / 3600)
    entries.append((l3_data_time[key] - l2_time[key]) / divider)

print "Runs: %s" % len(entries)

file = os.path.join(args.out, 'diff_%s_%s.png' % (args.l2dataset, args.l3dataset))

max_bin = args.xmax

if max_bin is None:
    max_bin = max(entries)

pylab.figure()
pylab.hist(entries, bins = range(0, max_bin), range = (0, 50))
pylab.xlabel('L3 %s Completion after L2 in %s' % (wg_name, unit))
pylab.ylabel('# of Runs')
pylab.savefig(file)

print "Output file written to %s" % file

