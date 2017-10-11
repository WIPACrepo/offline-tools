
import argparse
import re
import os
import sys
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2017/')
from databaseconnection import DatabaseConnection
from libs.logger import DummyLogger

parser = argparse.ArgumentParser()
parser.add_argument('--file', required = True, help = "logfile", type = str)
parser.add_argument('--start-line', required = False, default = 0, help = "start at line...", type = int)
args = parser.parse_args()

error_files = []

md5_mismatch_c = re.compile(r'MD5\ check\ sum\ mismatch\ for\ ([\/a-zA-Z0-9\.\-\_]+)')
stream_error_c = re.compile(r'ERROR\:\ checks\(229\)\:\   File\ ([\/\-\_\.a-zA-Z0-9]+)\ is\ corrupted\: Command')

def find_md5_mismatch(text):
    result = md5_mismatch_c.search(text)

    if result is not None:
        error_files.append(result.groups()[0])
        return True
    else:
        return False

def find_stream_errors(text):
    result = stream_error_c.search(text)

    if result is not None:
        error_files.append(result.groups()[0])
        return True
    else:
        return False

with open(args.file, 'r') as f:
    for line, text in enumerate(f):
        if line < args.start_line:
            continue

        if not find_stream_errors(text):
            if find_md5_mismatch(text):
                print 'Found md5 mismatch: {}'.format(error_files[-1])
        else:
            print 'Found stream error: {}'.format(error_files[-1]) 

print '--------------------------'
print 'Found {} bad files'.format(len(error_files))
print '--------------------------'
print 'SQL in orde rto restart those jobs:'

# Get job_ids
error_files = ["'{}'".format(os.path.basename(f)) for f in error_files]

sql = '''SELECT 
    job_id
FROM
    i3filter.urlpath u
        JOIN
    i3filter.job j ON j.dataset_id = u.dataset_id
        AND j.queue_id = u.queue_id
WHERE
    name IN ({})'''.format(','.join(error_files))

db = DatabaseConnection.get_connection('dbs4', DummyLogger())
jids = [str(i['job_id']) for i in db.fetchall(sql, UseDict = True)]

sql = "UPDATE i3filter.job SET status = 'WAITING', failures = 0 WHERE job_id IN ({})".format(','.join(jids))
print sql
