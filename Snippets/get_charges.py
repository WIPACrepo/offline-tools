import argparse
import os

from icecube import icetray, dataclasses, dataio
from I3Tray import *
import glob

parser = argparse.ArgumentParser()
parser.add_argument('--file', required = True, help = "The L2 file", type = str)
parser.add_argument('--gcd', required = True, help = "The GCD file", type = str)
parser.add_argument('--out', required = True, help = "Output file name", type = str)
args = parser.parse_args()

infiles = [args.gcd, args.file]

print('Infiles: {0}'.format(infiles))

def qtot(frame):
    charge = 0
    
    try:
        pulses = dataclasses.I3RecoPulseSeriesMap.from_frame(frame, "SplitInIcePulses")
    except:
        pulses = dataclasses.I3RecoPulseSeriesMap.from_frame(frame, "OfflinePulses") 

    for om, pulseList in pulses:
        for pulse in pulseList:
            charge += pulse.charge

    frame["TotalCharge_split"] = dataclasses.I3Double(charge)

def substreamfilter(frame):
    return frame['I3EventHeader'].sub_event_stream == 'in_ice' or frame['I3EventHeader'].sub_event_stream == 'InIceSplit'

def filtermaskfilter(frame):
    return 'FilterMask' in frame

tray = I3Tray()
tray.Add('I3Reader', 'reader', FilenameList = infiles)
tray.Add(substreamfilter, Streams = [icetray.I3Frame.Physics])
tray.Add(filtermaskfilter, Streams = [icetray.I3Frame.Physics])
tray.Add(qtot, Streams = [icetray.I3Frame.Physics])
tray.Add('Keep', keys = ['TotalCharge_split', 'I3EventHeader'])
tray.Add('I3Writer', FileName = args.out)
tray.AddModule("TrashCan","trash")
tray.Execute()
tray.Finish()

print('Done')
