
"""
Makes symlinks relative. That means: If a symlink looks like /a/b/c/d -> /a/b/e/f it will be changed to /a/b/c/d -> ../e/f.
"""

import os
import argparse
from glob import glob

def find_symlinks(path, verbose = False):
    files = [f for f in glob(path) if os.path.islink(f)]

    if verbose:
        print "path: %s" % path
        print "found symlinks: %s" % files

    return files

def make_symlink_relative(path, dryrun = True):
    dst = os.readlink(path)
    if not os.path.isabs(dst):
        # already relative
        print "Skip %s since it's already a relative symlink" % path
        return

    rel = os.path.relpath(dst, os.path.dirname(path))

    if not dryrun:
        os.unlink(path)
        os.symlink(rel, path)

    print "symlink %s -> %s" % (path, rel)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('--dryrun', help = "Just pretending. Don't do actual work.", dest = "dryrun", action = "store_true", default = False)
    parser.add_argument('-n', help = "Make only n symlinks relative. 0 means no limit", dest = "n", type = int, default = 0)
    parser.add_argument(help = "Path to check for sym links (use * etc)", dest = "path", type = str, nargs = '+')

    args = parser.parse_args()

    links = []

    for p in args.path:
        links.extend(find_symlinks(p))

    print "Found %s symlinks" % (len(links))

    if args.n > 0:
        print "Make only %s symlinks relative" % args.n

    counter = 0
    for l in links:
        make_symlink_relative(l, args.dryrun)

        if counter % 10 == 0:
            print "%s%%\t%s/%s" % (round(float(counter) / len(links) * 100, 2), counter, len(links))

        counter = counter + 1

        if args.n > 0 and counter >= args.n:
            break

    if len(links):
        print "%s%%\t%s/%s" % (round(float(counter) / len(links) * 100, 2), counter, len(links))
    else:
        print "No symlinks found"
