
import sys
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
import libs.databaseconnection
import libs.logger
import matplotlib
matplotlib.use('Agg') 
import pylab
import pprint
import argparse
import numpy
import re

def make_host(host):
    splithost = host.split('.')
    name = splithost[-2:]
    if name[-1] == 'local':
        return name[-1]
    elif name[-1].startswith('tier'):
        return name[-1]
    elif re.match(r"^n[0-9]{4}$", name[0]):
        return 'hyak.washington.edu'
    elif len(splithost) > 3:
        if splithost[-3] == 'icecube':
            return '.'.join(splithost[-3:])
    
    return '.'.join(name)

def sort_data(data):
    sdata = {}

    for row in data:
        host = make_host(row['host'])
        if host not in sdata:
            sdata[host] = {'i3exec runtime': []}

        sdata[host]['i3exec runtime'].append(row['value'])

    return sdata

def make_plot(data, filename, dataset_id):
    fig, ax = matplotlib.pyplot.subplots()
    
    index = numpy.arange(len(data.keys()))
    bar_width = 0.35
    
    opacity = 0.4
    error_config = {'ecolor': '0.3'}
 
    keys = data.keys()

    alldata = []
    for host, value in data.iteritems():
        alldata.extend(value['i3exec runtime'])

    mean = numpy.mean(numpy.array(alldata))

    rects1 = matplotlib.pyplot.bar(index,
                     [data[h]['exec_average'] for h in keys],
                     bar_width,
                     alpha = opacity,
                     color = 'b',
                     yerr = [data[h]['exec_std'] for h in keys],
                     error_kw = error_config,
                     label = 'i3exec runtime')
    
    matplotlib.pyplot.xlabel('Host')
    matplotlib.pyplot.ylabel('Execution Time in Seconds')
    matplotlib.pyplot.title('Execution Time by Host (dataset %s)' % dataset_id)
    matplotlib.pyplot.xticks(index + bar_width / 2, keys, rotation = 'vertical')

    matplotlib.pyplot.axhline(mean, color = 'r', linestyle = 'dashed', linewidth = 2, label = "Dataset average")

    matplotlib.pyplot.legend()
    
    matplotlib.pyplot.tight_layout()
    #matplotlib.pyplot.show()
    pylab.savefig(filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', required = True, help = "Dataset id", type = int)
    parser.add_argument('--out', required = True, help = "Output filename", type = str)
    args = parser.parse_args()
    
    slogger = libs.logger.DummyLogger()
    slogger.silence = True
    db = libs.databaseconnection.DatabaseConnection.get_connection('dbs4', slogger)

    sql = """SELECT 
    `host`, `name`, `value`
FROM
    i3filter.job j
        JOIN
    i3filter.job_statistics s ON s.dataset_id = j.dataset_id
        AND s.queue_id = j.queue_id
WHERE
    `host` IS NOT NULL AND name = 'i3exec runtime' AND j.dataset_id = %s""" % args.dataset

    data = sort_data(db.fetchall(sql, UseDict = True))

    for host, value in data.iteritems():
        tmp = numpy.array(value['i3exec runtime'])

        #value['exec_average'] = sum(value['i3exec runtime']) / len(value['i3exec runtime'])
        value['exec_average'] = numpy.mean(tmp)
        value['exec_std'] = numpy.std(tmp)
        print "%s\t%s +- %s" % (host, value['exec_average'], value['exec_std'])

    make_plot(data, args.out, args.dataset)
