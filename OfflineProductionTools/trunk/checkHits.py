#!/usr/bin/env python

import os, sys
from icecube import icetray, dataclasses, dataio


f = dataio.I3File("R_PFFilt_PhysicsFiltering_Run00124798_Subrun00000000_00000000.i3")

while f.more():
    ff = f.pop_frame()
    if 'InIcePulses' in ff:
        II = ff['InIcePulses']
        k = II.apply(ff)
        kk = [str(sk) for sk in k.keys()]
    #    print kk
        if 'OMKey(86,9,0)' in kk:
            print ff['I3EventHeader']
            #print k
            break
