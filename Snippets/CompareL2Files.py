
import argparse
import os

from icecube import icetray, dataclasses, dataio
from I3Tray import *

parser = argparse.ArgumentParser()
parser.add_argument('--files', nargs = 2, required = True, help = "The L2 files", type = str)
parser.add_argument('--gcds', nargs = 2, required = True, help = "The GCD files", type = str)
parser.add_argument('--tmp-folder', required = True, help = "Print only affected datasets", type = str)
parser.add_argument('--no-skip', action = 'store_true', default = False, help = "Skip i3 file generation")
args = parser.parse_args()

def qtot(frame):
    charge = 0
    pulses = dataclasses.I3RecoPulseSeriesMap.from_frame(frame, "InIcePulses")

    for om, pulseList in pulses:
        for pulse in pulseList:
            charge += pulse.charge

    frame["TotalCharge_split"] = dataclasses.I3Double(charge)

def create_i3_file(gcd, l2, tmp_dir, i, skip):
    file_name = os.path.join(tmp_dir, '{0}_{1}'.format(i, os.path.basename(l2)))
    print 'Create i3 file: {0}'.format(file_name)

    if skip:
        return file_name

    tray = I3Tray()
    tray.Add('I3Reader', 'reader', FilenameList = [gcd, l2])
    tray.Add(qtot, Streams = [icetray.I3Frame.Physics])
    tray.Add('Keep', keys = ['TotalCharge_split', 'I3EventHeader'])
    tray.Add('I3Writer', FileName = file_name)
    tray.AddModule("TrashCan","trash")
    tray.Execute()
    tray.Finish()

    del tray

    return file_name

files = [(args.gcds[i], args.files[i]) for i in range(2)]
tmp_files = []

for i, (gcd, l2) in enumerate(files):
    tmp_files.append(create_i3_file(gcd, l2, args.tmp_folder, i, not args.no_skip))

def compare(files):
    print 'files: {0}'.format(files)

    files = [dataio.I3File(f) for f in files]

    frame_counter = [0, 0]

    additional_events = [[], []]

    qtot = [[], []]

    ok = True
    while ok:
        oks = [False, False]

        for i, f in enumerate(files):
            other = int(not i)

            if f.more():
                frame = f.pop_physics()
                oks[i] = True
                frame_counter[i] += 1

                if frame is None:
                    print 'Frame is None'
                    continue

                event_id = frame['I3EventHeader'].event_id

                if event_id in additional_events[other]:
                    additional_events[other].remove(event_id)
                else:
                    additional_events[i].append(event_id)

                qtot[i].append(frame['TotalCharge_split'].value)

        for o in oks:
            ok = ok and o

    print 'frame counters: {0}'.format(frame_counter)
    #print 'Additional Events: {0}'.format(additional_events)

    qtmin = min(min(qtot[0]), min(qtot[1]))
    qtmax = max(max(qtot[0]), max(qtot[1]))

    import random
    import numpy
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot
 
    bins = numpy.linspace(qtmin, qtmax, 100)
  

    # H1
    pyplot.yscale('log', nonposy='clip')
    h1 = pyplot.hist(qtot[0], bins, alpha = 0.5, label = 'file 1')
    pyplot.legend(loc = 'upper right')
    pyplot.savefig('/home/joertlin/public_html/tmp/pass2cpr_1_2.png')

    # H2
    pyplot.figure()
    pyplot.yscale('log', nonposy='clip')
    h2 = pyplot.hist(qtot[1], bins, alpha = 0.5, label = 'file 2')
    pyplot.legend(loc = 'upper right')
    pyplot.savefig('/home/joertlin/public_html/tmp/pass2cpr_2_2.png')

    # H2
    pyplot.figure()
    pyplot.yscale('log', nonposy='clip')
    pyplot.hist(qtot[0], bins, alpha = 0.5, label = 'file 1')
    pyplot.hist(qtot[1], bins, alpha = 0.5, label = 'file 2')
    pyplot.legend(loc = 'upper right')
    pyplot.savefig('/home/joertlin/public_html/tmp/pass2cpr_2.png')

    # Diff
    import math

    data = (h1[0] / h2[0])
    data = [1 if math.isnan(d) or math.isinf(d) else d for d in data]

    print data

    pyplot.figure()
    pyplot.plot(h2[1][:-1], data, 'bo')
    pyplot.savefig('/home/joertlin/public_html/tmp/pass2cpr_diff_2.png')
    


compare(tmp_files)


