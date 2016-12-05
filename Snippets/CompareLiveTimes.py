
import sys
import os

sys.path.append('/data/user/i3filter/SQLServers_n_Clients')
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
import SQLClient_dbs4 as dbs4
import libs.databaseconnection
import libs.logger
from CompareGRLs import read_file as read_grl
import matplotlib
matplotlib.use('Agg') 
import pylab

grl_files = ['/data/exp/IceCube/2015/filtered/level2/IC86_2015_GoodRunInfo.txt', '/data/exp/IceCube/2014/filtered/level2/IC86_2014_GoodRunInfo.txt', '/data/exp/IceCube/2013/filtered/level2/IC86_2013_GoodRunInfo.txt']

def create_comparison(grl_file):
    grl = read_grl(grl_file)

    runs = grl.keys()

    grl_livetime = {}
    for key, row in grl.iteritems():
        try:
            grl_livetime[str(row[0])] = float(row[3])
        except IndexError:
            print "Remove run %s (%s)" % (row[0], row)
            runs.remove(row[0])

    sql = """SELECT 
    run_id, SUM(livetime) AS `livetime`
FROM
    i3filter.sub_runs
WHERE
    run_id IN (%s)
GROUP BY run_id;""" % (','.join(runs))

    filter_db = libs.databaseconnection.DatabaseConnection.get_connection('filter-db', libs.logger.DummyLogger())

    query = filter_db.fetchall(sql, UseDict = True)

    real_livetime = {}

    for row in query:
        real_livetime[str(row['run_id'])] = float(row['livetime'])

    return real_livetime, grl_livetime

for file in grl_files:
    real_livetime, grl_livetime = create_comparison(file)

    runs = real_livetime.keys()


#    s1 = set(real_livetime.keys())
#    s2  = set(grl_livetime.keys())
#
#    print real_livetime.keys()
#    print grl_livetime.keys()
#
#    print s1 - s2
#    print s2 - s1

    x = [real_livetime[r] for r in runs]
    y = [grl_livetime[r] for r in runs]

    diff = [real_livetime[r] - grl_livetime[r] for r in runs]

    cpr_file = os.path.basename(file) + '.cpr.png'

#    pylab.plot(x, y, 'ro')
#    pylab.axis([0, max(x), 0, max(y)])
#    pylab.xlabel('real livetime')
#    pylab.ylabel('GRL livetime')

    minBin = int(min(diff)) - 10
    maxBin = int(max(diff)) + 10

    pylab.figure()
    pylab.hist(diff, bins = range(minBin, maxBin), range = (-50, 50))
    pylab.xlabel('gaps livetime - GRL livetime')
    pylab.ylabel('# of Runs')
    pylab.yscale('log')
    #pylab.semilogy()
    pylab.axis([minBin, maxBin, 1e-1, 2000])
    pylab.savefig(cpr_file)

    print "min = %s" % min(diff)
    print "max = %s" % max(diff)
    

    # cpr:
    print "Run #\tReal LT\tGRL LT\tReal LT - GRL LT"
    for r in runs:
        print "Run %s\t%s\t%s\t%s" % (r, real_livetime[r], grl_livetime[r], (real_livetime[r] - grl_livetime[r]))

