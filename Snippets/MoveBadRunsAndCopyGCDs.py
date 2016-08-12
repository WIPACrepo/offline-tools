
import os
import glob
import shutil

cwd = os.getcwd()

# get run dirs
run_dirs = glob.glob(os.path.join(cwd, 'Run00*_[1-9][0-9]'))

print "Move dirs: %s" % run_dirs

for d in run_dirs:
    # find GCD file
    gcd = glob.glob(os.path.join(d, '*GCD*'))[0]
    real_gcd = os.path.realpath(gcd)

    print "Remove %s" % gcd
    os.unlink(gcd)

    print "Copy %s to %s" % (real_gcd, d)
    shutil.copy(real_gcd, d)

    new_name = "%s_bad_GCD" % d
    print "Move %s to %s" % (d, new_name)
    shutil.move(d, new_name)

    print "========= NEXT DIR ======================="

