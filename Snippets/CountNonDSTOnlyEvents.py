
import glob
from icecube import dataio

# Adjust date and run number in path to switch to another run
files = glob.glob('/data/exp/IceCube/2016/filtered/level2/0606/Run00128008/*_Subrun00000*.i3.bz2')

# Search for 'DAQ' events or 'Physics' events?
event_type = 'DAQ'

# filter IT and EHE
files = [f for f in files if 'IT' not in f and 'EHE' not in f]

files.sort()

total_events = 0
events = 0
file_count = 0

for file in files:
    i3file = dataio.I3File(file)

    print "Counting %s events in %s (%s/%s)..." % (event_type, file, file_count + 1, len(files))

    while i3file.more():
        frame = None

        if event_type == 'DAQ':
            frame = i3file.pop_daq()
        elif event_type == 'Physics':
            frame = i3file.pop_physics()
        else:
            print "Option '%s' not known... exit." % event_type
            exit()

        total_events = total_events + 1

        # non-DST-only events have a FilterMask (Physics) or InIceRawData (DAQ)
        if event_type == 'DAQ' and frame is not None:
            if 'InIceRawData' in frame:
                events = events + 1
        elif event_type == 'Physics' and frame is not None:
            if 'FilterMask' in frame:
                events = events + 1
        

    print "non-DST-only/total number of events: %s/%s" % (events, total_events)

    file_count = file_count + 1

print "There are %s non-DST-only events (%s) in %s files." % (events, event_type, len(files))
print "The total number of events were %s." % total_events

