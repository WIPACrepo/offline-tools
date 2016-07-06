
import glob
from icecube import dataio

files = glob.glob('/data/exp/IceCube/2016/filtered/level2/0606/Run00128008/*_Subrun00000*.i3.bz2')

# filter IT and EHE
files = [f for f in files if 'IT' not in f and 'EHE' not in f]

files.sort()

total_events = 0
events = 0
file_count = 0

for file in files:
    i3file = dataio.I3File(file)

    print "Counting events in %s (%s/%s)..." % (file, file_count + 1, len(files))

    while i3file.more():
        frame = i3file.pop_physics()

        total_events = total_events + 1

        if 'FilterMask' in frame:
            events = events + 1

    print "non-DST-only/total number of events: %s/%s" % (events, total_events)

    file_count = file_count + 1

print "There are %s non-DST-only events in %s files." % (events, len(files))
print "The total number of events were %s." % total_events

