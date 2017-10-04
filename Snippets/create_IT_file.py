
from icecube import icetray, dataclasses, dataio
from I3Tray import *
from icecube.filterscripts.offlineL2 import SpecialWriter

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-o', help = "Output file", type = str, required = True)
parser.add_argument('-i', help = "Input file", type = str, required = True)
args = parser.parse_args()

print 'Read {}'.format(args.i)
print 'Create IT file named {}'.format(args.o)

tray = I3Tray()
tray.Add(dataio.I3Reader, "reader", filenamelist=[args.i])

# this needs to be the last output
tray.AddModule('Delete','deleteicetopextrakeys',
    keys=['InIceRawData','CleanInIceRawData']
)

tray.AddSegment(SpecialWriter.IceTopWriter, "write_icetop",
    Filename = args.o
)

tray.AddModule('TrashCan', 'can')
tray.Execute()
tray.Finish()

del tray

