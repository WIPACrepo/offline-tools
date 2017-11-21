#!/usr/bin/env python

from I3Tray import *

from icecube import icetray, dataclasses, dataio
from icecube.phys_services import spe_fit_injector
from icecube.icetray import OMKey

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-g', required = True, help = "Input GCD file", type = str)
parser.add_argument('-o', required = True, help = "Output GCD file", type = str)
parser.add_argument('--add-doms', default = [], nargs = '+', required = False, help = "Add those DOMS, e.g. --add-doms 42,21 21,42", type = str)
parser.add_argument('--rm-doms', default = [], nargs = '+', required = False, help = "Remove those DOMS, e.g. --add-doms 42,21 21,42", type = str)
args = parser.parse_args()

add_doms = []
for o in args.add_doms:
    d = o.split(',')

    add_doms.append(OMKey(int(d[0]), int(d[1])))

rm_doms = []
for o in args.rm_doms:
    d = o.split(',')

    rm_doms.append(OMKey(int(d[0]), int(d[1])))

print 'Add the following DOMs to the BDL: {}'.format(add_doms)
print 'Remove the following DOMs from the BDL: {}'.format(rm_doms)

def to_i3_class(l):
    nl = dataclasses.I3VectorOMKey()
    for e in l:
        nl.append(e)

    return nl

def modify_bdl(frame):
    bdl = list(frame['BadDomsList'])
    bdl_slc = list(frame['BadDomsListSLC'])

    print "BDLs before update:"
    print "BDL ({0}):     {1}".format(len(bdl), bdl)
    print "BDL SLC ({0}): {1}".format(len(bdl_slc), bdl_slc)

    bdl.extend(add_doms)
    bdl_slc.extend(add_doms)

    for o in rm_doms:
        bdl.remove(o)
        bdl_slc.remove(o)

    bdl = sorted(list(set(bdl)))
    bdl_slc = sorted(list(set(bdl_slc)))

    print "BDLs after update:"
    print "BDL ({0}):     {1}".format(len(bdl), bdl)
    print "BDL SLC ({0}): {1}".format(len(bdl_slc), bdl_slc)

    del frame['BadDomsList']
    del frame['BadDomsListSLC']

    frame['BadDomsList'] = to_i3_class(bdl)
    frame['BadDomsListSLC'] = to_i3_class(bdl_slc)

tray = I3Tray()

tray.AddModule("I3Reader", "reader", filenamelist=[args.g])

tray.Add(modify_bdl, 'modbdl', Streams = [icetray.I3Frame.DetectorStatus])

tray.AddModule("I3Writer", filename = args.o, CompressionLevel = 19)
tray.Execute()

print 'Done'
