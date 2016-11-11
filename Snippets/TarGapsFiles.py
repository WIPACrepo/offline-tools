
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
        dataset_selection = "u.dataset_id IN (%s) AND" % ','.join([str(d) for d in datasets])

    sql = """SELECT 
    run_id, u.dataset_id, path, name, urlpath_id, u.queue_id
FROM
    i3filter.urlpath u
        JOIN
    i3filter.run r ON r.queue_id = u.queue_id
        AND r.dataset_id = u.dataset_id
WHERE
        %s
        name LIKE '%%gaps.txt'""" % dataset_selection

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

def skip_run(run_id, gaps_files, deleteonly = False):
    # Check if tar file already exists
    tar_file = os.path.join(os.path.dirname(gaps_files[-1]['path']), "Run00%s_GapsTxt.tar" % run_id)

    exists = os.path.isfile(tar_file)

    print "Check if %s exists (%s)" % (tar_file, exists)

    if exists:
        # If we want to create tar files, then we know that we can skip this run. If we want to delete gaps file, we need further checks
        if not deleteonly:
            return True
    elif deleteonly:
        # If the tar-ed file doesn't exist and we only want to delete gaps-files, we want to skip this run
        return True

    print "Check if gaps files exists"

    for file in gaps_files:
        if not os.path.isfile(file['path']):
            print "******** File %s does not exist" % file
            return True

    return False

def tar_gaps(run_id, gaps_files, dryrun):
    dest = os.path.join(os.path.dirname(gaps_files[-1]['path']), "Run00%s_GapsTxt.tar" % run_id)

    print "Tar file: %s" % dest

    if not dryrun:
        with tarfile.open(dest, 'w') as tar:
            for file in gaps_files:
                tar.add(file['path'], arcname = os.path.basename(file['path']))
    
    return dest

def fix_db(dataset_id, tar_file, gaps_files, deleteonly, dryrun):
    if not deleteonly:
        tar_path = 'file:' + os.path.dirname(tar_file)
        tar_name = os.path.basename(tar_file)
    
        size = os.path.getsize(tar_file)
        md5sum = FileTools(tar_file).md5sum()

    # The tar gaps files get the queue id of the last sub run
    queue_id = gaps_files[-1]['queue_id']

    sql = "UPDATE urlpath SET transferstate = 'DELETED' WHERE urlpath_id IN (%s)" % ','.join(str(f['uid']) for f in gaps_files)

    if deleteonly:
        sql = "DELETE FROM urlpath WHERE urlpath_id IN (%s)" % ','.join(str(f['uid']) for f in gaps_files)

    #print "UPDATE SQL statement: %s" % sql
    
    if not dryrun:
        dbs4.MySQL().execute(sql)

    if not deleteonly:
        sql = "INSERT INTO urlpath (name, path, type, dataset_id, queue_id, md5sum, size, transferstate) VALUES ('%s', '%s', '%s', %s, %s, '%s', %s, '%s')" % (
            tar_name, tar_path, 'PERMANENT', dataset_id, queue_id, md5sum, size, 'WAITING'
        )
    
        #print "INSERT SQL statement: %s" % sql
    
        if not dryrun:
            dbs4.MySQL().execute(sql)

def move_gaps_files(gaps_files, dest, dryrun):
    for file in gaps_files:
        d = os.path.join(dest, os.path.basename(file['path']))
        print "move %s to %s" % (file['path'], d)

        if not dryrun:
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
parser.add_argument('--datasetinfo', action = 'store_true', default = False, help = "Print only affected datasets")
parser.add_argument('--dryrun', action = 'store_true', default = False, help = "Run the script w/o doing something")
parser.add_argument('--deleteonly', action = 'store_true', default = False, help = "Do not create tar files. Do only delete *_gaps.txt files. Note: The gaps files are only deleted if the tar file already exists")
parser.add_argument('-n', nargs = "?", help = "Number of runs that should be processed", type = int)
parser.add_argument('--gapsdest', nargs = "?", help = "Destination of the moved gaps files. Default is /data/user/i3filter/tmp/gaps_files", type = str, default = '/data/user/i3filter/tmp/gaps_files')
args = parser.parse_args()

max_runs = args.n

runs = get_runs(datasets = args.datasets)

# Show list of affected datasets
show_datasets(runs)

if args.datasetinfo:
    exit()

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

        if skip_run(run_id, gaps_files, deleteonly = args.deleteonly):
            if args.deleteonly:
                print "Skip run because no tar file exists"
            else:
                print "Skip run because tar-ed gaps file already exist"

            continue

        tar_file = None
        if not args.deleteonly:
            tar_file = tar_gaps(run_id, gaps_files, dryrun = args.dryrun)
        
        fix_db(dataset_id, tar_file, gaps_files, dryrun = args.dryrun, deleteonly = args.deleteonly)

        move_gaps_files(gaps_files, dest = args.gapsdest, dryrun = args.dryrun)

        print "\n"

        if max_runs is not None and counter >= max_runs:
            break

    break


