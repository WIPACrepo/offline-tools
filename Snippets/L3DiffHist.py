
import sys
sys.path.append('/data/user/i3filter/SQLServers_n_Clients')
import SQLClient_dbs4

import matplotlib
matplotlib.use('Agg') 
import pylab

dbs4 = SQLClient_dbs4.MySQL()

l2 = dbs4.fetchall("""SELECT run_id, MAX(UNIX_TIMESTAMP(status_changed)) AS `time` FROM run r JOIN job j ON r.queue_id = j.queue_id AND r.dataset_id = j.dataset_id WHERE r.dataset_id = 1883 AND status_changed IS NOT NULL AND run_id > 127315 GROUP BY run_id""", UseDict = True)

l3_muon = dbs4.fetchall("""SELECT run_id, MAX(UNIX_TIMESTAMP(status_changed)) AS `time` FROM run r JOIN job j ON r.queue_id = j.queue_id AND r.dataset_id = j.dataset_id WHERE r.dataset_id = 1885 AND status_changed IS NOT NULL AND run_id > 127315 GROUP BY run_id""", UseDict = True)

keys = list(set([r['run_id'] for r in l2]) & set([r['run_id'] for r in l3_muon]))

sorted(keys)

l2_time = {}
for run in l2:
    l2_time[run['run_id']] = run['time']

l3_muon_time = {}
for run in l3_muon:
    l3_muon_time[run['run_id']] = run['time']

entries = []

print "# Run Id\tdelta T"
for key in keys:
#    if (l3_muon_time[key] - l2_time[key]) / 3600 > 700:
#        print "%s\t%s" % (key, (l3_muon_time[key] - l2_time[key]) / 3600)
    entries.append((l3_muon_time[key] - l2_time[key]) / 3600 / 24)

pylab.figure()
pylab.hist(entries, bins = max(entries), range = (0, 50))
pylab.xlabel('L3 Muon Completion after L2 in Days')
pylab.ylabel('# of Runs')
pylab.savefig('diff.png')

