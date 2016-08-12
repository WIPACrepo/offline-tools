
from icecube import dataio, dataclasses
from icecube.icetray import OMKey
from I3Tray import *

infile = 'Level2_IC86.2015_data_Run00127525_19_227_GCD.i3.gz'
outfile = "new_%s" % infile

i3file = dataio.I3File(infile)

# Get D frame
frames = {}
frames['G'] = i3file.pop_frame()
frames['C'] = i3file.pop_frame()
frames['D'] = i3file.pop_frame()

# Get BDLs
bdl = frames['D']['BadDomsList']
bdlSLC = frames['D']['BadDomsListSLC']

print "Old list length (BDL, BDLSLC): %s, %s" % (len(bdl), len(bdlSLC))

# Add the additional bad DOM
newBadDOMs = [OMKey(80, 60)]

bdl.extend(newBadDOMs)
bdlSLC.extend(newBadDOMs)

sorted(bdl)
sorted(bdlSLC)

print "New list length (BDL, BDLSLC): %s, %s" % (len(bdl), len(bdlSLC))

# Rewrite GCD
def AddBDL(frame, name, l):
    frame[name] = l

tray = I3Tray()
tray.Add('I3Reader', 'GCDReader', filename = infile)

tray.Add("Delete", keys = ['BadDomsList', 'BadDomsListSLC'])

tray.Add(AddBDL, 'AddBDL',
            name = 'BadDomsList',
            l = bdl,
            Streams = [icetray.I3Frame.DetectorStatus])

tray.Add(AddBDL, 'AddBDLSLC',
            name = 'BadDomsListSLC',
            l = bdlSLC,
            Streams = [icetray.I3Frame.DetectorStatus])

tray.Add('I3Writer', 'GCDWriter',
    FileName = outfile,
    Streams = [ icetray.I3Frame.Geometry, # ! Only write the GCD frames
                icetray.I3Frame.Calibration,
                icetray.I3Frame.DetectorStatus ]
    )   

tray.Execute()

tray.Finish()


