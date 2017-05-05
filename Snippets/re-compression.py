
import argparse
import os
import re

from icecube import dataio, dataclasses, icetray
from I3Tray import *

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2017/')
from libs.files import File
from libs.databaseconnection import DatabaseConnection
from libs.logger import DummyLogger

parser = argparse.ArgumentParser()
parser.add_argument('--files', nargs = "+", help = "Files to re-compress", type = str, required = True)
parser.add_argument('--outdir', help = "Destination of the re-compressed files", type = str, required = True)
parser.add_argument('--dryrun', action = "store_true", help = "Do not do anything", default = False)
args = parser.parse_args()

def re_compress(f, outdir, dryrun):
    outfile_info = os.path.splitext(os.path.basename(f))

    if outfile_info[1] not in ['.bz2', '.gz', '.i3']:
        print '\tFile {0} does not have an expected compression'.format(f)
        return f

    if outfile_info[1] == '.i3':
        outfile = os.path.join(outdir, os.path.basename(f) + '.zst')
    else:
        outfile = os.path.join(outdir, outfile_info[0] + '.zst')

    if not dryrun:
        tray = I3Tray()
        tray.Add('I3Reader', 'reader', FileName = f)
        tray.Add('I3Writer', 'writer', FileName = outfile, CompressionLevel = 19)
    
        tray.AddModule('TrashCan', 'can')
        tray.Execute()
        tray.Finish()
        del tray

    print '\t{0} has been created'.format(outfile)
    return outfile

def update_db(old_path, new_path, db, dryrun):
    f = File(new_path, DummyLogger())

    sql = """
        UPDATE i3filter.urlpath
        SET
            name = '{name}',
            size = {size},
            md5sum = '{md5}'
        WHERE
            name = '{old_name}' AND
            path = '{path}'
    """.format(name = f.get_name(), size = f.size(), md5 = f.md5(), old_name = os.path.basename(old_path), path = f.get_dirname())

    print 'SQL: {0}'.format(sql)

    if not dryrun:
        db.execute(sql)

def rename(path, dryrun):
    if re.match('^.*Subrun[0]{8}\_[0-9]{8}\.[a-z0-9]+$', os.path.basename(path)):
        # Filename has correct format
        print '\tNo renaming'
        return path

    new_name = 'Subrun00000000_'.join(os.path.basename(path).split('Subrun'))
    new_path = os.path.join(os.path.dirname(path), new_name)

    print '\tRename {0}\n\t    -> {1}'.format(path, new_path)

    if not dryrun:
        os.rename(path, new_path)

    return new_path
    

db = DatabaseConnection.get_connection('dbs4', DummyLogger())

counter = 0
for f in args.files:
    counter += 1

    print '[{0} / {1}]\tProcess {2}'.format(counter, len(args.files), f)

    renamed_path = rename(f, args.dryrun)

    print '\tre-compress {0}'.format(f)
    new_path = re_compress(renamed_path, args.outdir, args.dryrun)

    if renamed_path != new_path:
        print '\tDelete {0}'.format(f)
        if not args.dryrun:
            os.remove(f)

    print '\tUpdate DB entries'
    update_db(f, new_path, db, args.dryrun)

