
from icecube import icetray, dataclasses, dataio
from I3Tray import *
from icecube.filterscripts.offlineL2 import SpecialWriter

f = '/data/exp/IceCube/2015/filtered/level2/0926/Run00126894/Level2_IC86.2015_data_Run00126894_Subrun00000081.i3.bz2'

def clip_events(frame):
    if not frame.Has('I3EventHeader'):
        raise Exception('No I3EventHeader')

    header = frame['I3EventHeader']
    return header.event_id > 27307156

tray = I3Tray()
tray.Add(dataio.I3Reader, "reader", filenamelist=[f])

tray.AddModule(clip_events, Streams = [icetray.I3Frame.Physics, icetray.I3Frame.DAQ]) 

tray.Add('I3Writer', 'writer', FileName = os.path.join('./data/', os.path.basename(f)))

tray.AddSegment(SpecialWriter.GapsWriter, "write_gaps",
    Filename = os.path.join('./data/', os.path.basename(f).replace('.i3.bz2', '_gaps.txt')),
    MinGapTime = 1
)

# this needs to be the last output
tray.AddModule('Delete','deleteicetopextrakeys',
    keys=['InIceRawData','CleanInIceRawData']
)
tray.AddSegment(SpecialWriter.IceTopWriter, "write_icetop",
    Filename= os.path.join('./data/', os.path.basename(f).replace('.i3.bz2', '_IT.i3.bz2'))
)
tray.AddModule('TrashCan', 'can')
tray.Execute()
tray.Finish()
del tray

