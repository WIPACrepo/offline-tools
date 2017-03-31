
import argparse
import os
from glob import glob
from CompareGRLs import read_file

from I3Tray import *

from icecube import icetray, dataclasses, dataio
from icecube.phys_services import spe_fit_injector

def get_gcd_file(path, run_id):
    files = glob(os.path.join(path, "Level2*%s*GCD*.i3.gz" % run_id))

    if len(files) != 1:
        raise Exception('Did not finmd exactly one GCD file: %s' % files)

    return files[0]

def get_good_runs(season):
    path = "/data/exp/IceCube/%s/filtered/level2/IC86_%s_GoodRunInfo.txt" % (season, season)
    grl = read_file(path)

    return grl

def existing_gcd_files(season):
    path = "/data/exp/IceCube/%s/filtered/level2pass2/AllGCD"
    existing_gcds = glob(os.path.join(path % season, 'Level2pass2_IC86.%s_data_Run00*GCD.i3.gz') % (season))
    existing_gcds.extend(glob(os.path.join(path % (season + 1), 'Level2pass2_IC86.%s_data_Run00*GCD.i3.gz') % (season)))

    return list(set([f.split('Run00')[1].split('_')[0] for f in existing_gcds]))

def generate_gcd_file(infile, outfile, spe_file):
    tray = I3Tray()
    
    tray.AddModule("I3Reader", "reader", filenamelist = [infile])
    
    # inject the SPE correction data into the C frame
    tray.AddModule(spe_fit_injector.I3SPEFitInjector, "fixspe",  Filename = spe_file)
    
    tray.AddModule("I3Writer", filename = outfile, CompressionLevel = 9)
    tray.Execute()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', help = "output folder", type = str, required = True)
    parser.add_argument('--season', help = "season, e.g. 2011, 2012, 2013.", type = int, required = True)
    parser.add_argument('--spe-correction-file', help = "The SPE correction file to be used", type = str, required = True)
    args = parser.parse_args()

    print "Get good run list"
    grl = get_good_runs(args.season)
    print "Found %s good runs in %s" % (len(grl.keys()), args.season)
    print ""

    print "Find existing GCD files"
    already_done = existing_gcd_files(args.season)
    print "Found %s runs that are already done" % len(already_done)
    print ""

    to_do = list(set(grl.keys()) - set(already_done))
    print "Need to generate %s GCD files" % len(to_do)
    print ""

    counter = 0
    print "Start generating files using SPE correction file = %s" % args.spe_correction_file
    for run_id in to_do:
        counter += 1

        data = grl[run_id]

        infile = get_gcd_file(data[7], run_id)
        outfile = os.path.join(args.out, os.path.basename(infile)).replace("Level2_", "Level2pass2")

        if os.path.isfile(outfile):
            print "[%s / %s]\tRun %s: outfile already exists: %s" % (counter, len(to_do), run_id, outfile)
        else:
            print "[%s / %s]\tRun %s: %s -> %s" % (counter, len(to_do), run_id, infile, outfile)
            generate_gcd_file(infile, outfile, args.spe_correction_file)

