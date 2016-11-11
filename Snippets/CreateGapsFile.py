
from icecube import dataio, dataclasses
import sys
import re
import os

if len(sys.argv) != 2:
    print "Expecting one argument (sub run i3 file)"
    exit(-1)

run_id_pattern = re.compile(r'^/.*Run00([0-9]{6}).*$')
sub_run_id_pattern = re.compile(r'^/.*Subrun0+([0-9]+).*$')

def GetRunIdFromPath(path, c):
    return c.search(path).groups()[0]

def GetSubRunIdFromPath(path, c):
    return int(c.search(path).groups()[0])

file = dataio.I3File(sys.argv[1])

print "File: %s" % sys.argv[1]

outfile = os.path.basename(sys.argv[1][:-len('.i3.bz2')] + '_gaps.txt')

print "Gaps file: %s" % outfile

run_id = GetRunIdFromPath(sys.argv[1], run_id_pattern)
sub_run = GetSubRunIdFromPath(sys.argv[1], sub_run_id_pattern)

first_frame = file.pop_frame()
first_header = first_frame['I3EventHeader']

last_frame = None
while file.more():
    last_frame = file.pop_frame()

last_header = last_frame['I3EventHeader']

live_time = (last_header.start_time - first_header.end_time) / 10**9

print 'Run: %s' % run_id
print 'First Event of File: %s %s %s' % (first_header.event_id, first_header.start_time.utc_year, first_header.start_time.utc_daq_time)
print 'Last Event of File: %s %s %s' % (last_header.event_id, last_header.start_time.utc_year, last_header.start_time.utc_daq_time)
print 'File Livetime: %s' % live_time

with open(outfile, 'w') as f:
    f.write('Run: %s\n' % run_id)
    f.write('First Event of File: %s %s %s\n' % (first_header.event_id, first_header.start_time.utc_year, first_header.start_time.utc_daq_time))
    f.write('Last Event of File: %s %s %s\n' % (last_header.event_id, last_header.start_time.utc_year, last_header.start_time.utc_daq_time))
    f.write('File Livetime: %s\n' % live_time)
    


