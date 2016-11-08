
import os
import sys
import shutil
import tarfile
import argparse

from glob import glob

sys.path.append('/data/user/i3filter/SQLServers_n_Clients')
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
import SQLClient_dbs4 as dbs4
from FileTools import FileTools

def get_runs(datasets = None):
    dataset_selection = ''

    if datasets is not None:
        dataset_selection = "AND u.dataset_id IN (%s)" % ','.join([str(d) for d in datasets])

    sql = """SELECT 
    run_id, u.dataset_id, path, name, urlpath_id, u.queue_id
FROM
    i3filter.urlpath u
        JOIN
    i3filter.run r ON r.queue_id = u.queue_id
        AND r.dataset_id = u.dataset_id
WHERE
    u.dataset_id < 1874
        %s
        AND name LIKE '%%gaps.txt'""" % dataset_selection

    dbresult = dbs4.MySQL().fetchall(sql, UseDict = True)

    result = {}

    for row in dbresult:
        path = os.path.join(row['path'][5:], row['name'])

        if row['dataset_id'] not in result:
            result[row['dataset_id']] = {}

        if row['run_id'] not in result[row['dataset_id']]:
            result[row['dataset_id']][row['run_id']] = []

        result[row['dataset_id']][row['run_id']].append({'queue_id': row['queue_id'], 'path': path, 'uid': row['urlpath_id']})

    return result

def skip_run(run_id, gaps_files):
    # Check if tar file already exists
    tar_file = os.path.join(os.path.dirname(gaps_files[-1]['path']), "Run00%s_GapsTxt.tar" % run_id)

    print "Check if %s exists" % tar_file

    return os.path.isfile(tar_file)

def tar_gaps(run_id, gaps_files):
    dest = os.path.join(os.path.dirname(gaps_files[-1]['path']), "Run00%s_GapsTxt.tar" % run_id)

    print "Tar file: %s" % dest

    with tarfile.open(dest, 'w') as tar:
        for file in gaps_files:
            tar.add(file['path'], arcname = os.path.basename(file['path']))

    return dest

def fix_db(dataset_id, tar_file, gaps_files):
    tar_path = 'file:' + os.path.dirname(tar_file)
    tar_name = os.path.basename(tar_file)

    size = os.path.getsize(tar_file)
    md5sum = FileTools(tar_file).md5sum()

    # The tar gaps files get the queue id of the last sub run
    queue_id = gaps_files[-1]['queue_id']

    sql = "UPDATE urlpath SET transferstate = 'DELETED' WHERE urlpath_id IN (%s)" % ','.join(str(f['uid']) for f in gaps_files)
    #print "UPDATE SQL statement: %s" % sql
    
    dbs4.MySQL().execute(sql)

    sql = "INSERT INTO urlpath (name, path, type, dataset_id, queue_id, md5sum, size, transferstate) VALUES ('%s', '%s', '%s', %s, %s, '%s', %s, '%s')" % (
        tar_name, tar_path, 'PERMANENT', dataset_id, queue_id, md5sum, size, 'WAITING'
    )

    #print "INSERT SQL statement: %s" % sql

    dbs4.MySQL().execute(sql)

def move_gaps_files(gaps_files):
    for file in gaps_files:
        d = os.path.join('/data/user/i3filter/tmp/gaps_files', os.path.basename(file['path']))
        print "move %s to %s" % (file['path'], d)
        shutil.move(file['path'], d)

def show_datasets(runs):
    def get_info(info, dataset_id):
        for i in info:
            if i['dataset_id'] == dataset_id:
                return i

        return 'N/A'

    print '****************************************************************'
    print "List of datasets (%s)" % len(runs)
    print '****************************************************************'

    sql = "SELECT * FROM i3filter.offline_dataset_season ds JOIN i3filter.offline_working_groups wg ON wg.wid = ds.working_group"
    dataset_info = dbs4.MySQL().fetchall(sql, UseDict = True)

    for dataset_id in runs:
        info = get_info(dataset_info, dataset_id)

        print "Dataset id (%s): %s" % (dataset_id, info)

    print "\n"

parser = argparse.ArgumentParser()
parser.add_argument('--datasets', nargs = "*", help = "Only specific datasets", type = int)
parser.add_argument('-n', nargs = "?", help = "Number of runs that should be processed", type = int)
args = parser.parse_args()

max_runs = args.n

runs = get_runs(datasets = args.datasets)

# Show list of affected datasets
show_datasets(runs)

counter = 0

l = sum([len(v) for k, v in runs.iteritems()])

for dataset_id, runs in runs.iteritems():
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    print "Start dataset id %s" % dataset_id
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

    for run_id, gaps_files in runs.iteritems():
        counter = counter + 1

        print '================================================================'
        print "[%s/%s] Run %s / dataset id %s" % (counter, l, run_id, dataset_id)
        print '================================================================'
        print "\n"

        if skip_run(run_id, gaps_files):
            print "Skip run because tar-ed gaps file already exist"
            continue

        tar_file = tar_gaps(run_id, gaps_files)
        fix_db(dataset_id, tar_file, gaps_files)
        move_gaps_files(gaps_files)

        print "\n"

        if max_runs is not None and counter >= max_runs:
            break

    break


