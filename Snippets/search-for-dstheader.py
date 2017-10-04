
from icecube import dataio, dataclasses, icetray

import sys

f = sys.argv[1]

print 'Input file: {}'.format(f)

i3f = dataio.I3File(f)

count = 0
last_dst_header = {'frame_count': None, 'frame': None}

dst_headers = 0

while i3f.more():
    frame = i3f.pop_frame()

    if 'I3DSTHeader' in frame.keys():
        last_dst_header['frame_count'] = count
        last_dst_header['frame'] = frame
        dst_headers += 1

    count += 1

print 'Total number of frames: {}'.format(count)
print 'Last DST header found in frame {}'.format(last_dst_header['frame_count'])

print 'Found DST headers: {}'.format(dst_headers)

print 'Frame keys: {}'.format(last_dst_header['frame'].keys())

