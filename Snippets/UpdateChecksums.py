
import sys
import argparse
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
import libs.databaseconnection
import libs.logger

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
from FileTools import FileTools

import os

parser = argparse.ArgumentParser()
parser.add_argument('--files', nargs = "+", help = "Files that should get a new checksum in DB. Only the filename is required, NOT the path!", type = str)
parser.add_argument('--ignoresize', action = "store_true", help = "Do not update file size", default = False)
args = parser.parse_args()

logger = libs.logger.DummyLogger()
logger.silence = True

db = libs.databaseconnection.DatabaseConnection.get_connection('dbs4', logger)

def get_paths(filenames, db):
    quoted_filenames = ["'%s'" % f for f in filenames]

    sql = "SELECT * FROM i3filter.urlpath WHERE name IN (%s)" % (','.join(quoted_filenames))

    data = db.fetchall(sql, UseDict = True)

    paths = []

    for row in data:
        paths.append({'name': row['name'], 'path': os.path.join(row['path'][5:], row['name']), 'size': row['size'], 'checksum': row['md5sum']})

    return paths

def add_new_checksum(paths, logger):
    for path in paths:
        print "Calculate size and new checksum for %s" % path['name']

        ft = FileTools(path['path'], logger)
        path['new_checksum'] = ft.md5sum()
        path['new_size'] = os.path.getsize(path['path'])

def update_db(paths, db):
    for d in paths:
        print "Update %s" % d['name']

        if d['checksum'] == d['new_checksum']:
            print "\tChecksum did not change"

        if d['size'] == d['new_size']:
            print "\tSize did not change"

        sql = "UPDATE i3filter.urlpath SET md5sum = '%s', size = %s WHERE name = '%s'" % (d['new_checksum'], d['new_size'], d['name'])

        print "\t%s" % sql

        db.execute(sql)

paths = get_paths(args.files, db)

add_new_checksum(paths, logger)

update_db(paths, db)


