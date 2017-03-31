
from glob import glob
import os
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', nargs = "+", help = "Specifies path(s) that should be deleted. You can use also glob symbols like *, ?, or [0-9]. Note: If there are a lot of files, put the path between quotes and the python script will expand it.", type = str, required = True)
    parser.add_argument('--dryrun', action = 'store_true', default = False, help = "Run the script w/o deleting files")
    parser.add_argument('-v', action = 'store_true', default = False, help = "Print any single path that gets deleted")
    args = parser.parse_args()
    
    print "** Looking for files:"

    files = []
    for path in args.path:
        t = glob(path)

        print "Checking %s: %s files" % (path, len(t))
        files.extend(t)

    print "** Deleting %s files:" % len(files)

    counter = 0
    for f in files:
        counter += 1

        if args.v:
            print "[%s / %s]\tDelete %s" % (counter, len(files), f)
        elif counter % 100 == 0:
            print "%s / %s files deleted" % (counter, len(files))

        if not args.dryrun:
            os.remove(f)

    print "** Done"
