##!/usr/bin/env python
#
#import os, sys
#from icecube import icetray, dataclasses, dataio
#
#
##f = dataio.I3File("/data/exp/IceCube/2014/filtered/PFFilt/1012/PFFilt_PhysicsFiltering_Run00125419_Subrun00000000_00000264.tar.bz2")
#
#f = sys.argv[1]
#
#print f
#
#raise "*"
#
#while f.more():
#    ff = f.pop_frame()
#    if 'I3SuperDST' in ff:
#        p = dataclasses.I3RecoPulseSeriesMap.from_frame(ff,"I3SuperDST")
#        kk = [str(sk) for sk in p.keys()]
#        #print kk
#        if 'OMKey(12,62,0)' in kk:
#            print ff['I3EventHeader']
#            #break
#            
            
#!/usr/bin/env python

from icecube import icetray, dataclasses, dataio
import sys, os

from optparse import OptionParser


# example command
# python checkDOMsWithHits.py -i Level2_IC86.2014_data_Run00124840_Subrun00000000.i3.bz2 -o object1 -o object2

# handling of command line arguments  
parser = OptionParser()
usage = """%prog [options]"""
parser.set_usage(usage)
parser.add_option("-i", "--infile", action="store", type="string", default="", 
                  dest="InputFile", help="Input i3 file to search")
parser.add_option("-d", "--dom", action="store", type="string", default="", 
                  dest="DOM", help="DOM to check for, e.g. format : 'OMKey(23,34,0)' ")



# get parsed args
(options,args) = parser.parse_args()

i3file = options.InputFile
if not os.path.isfile(i3file):
    print "can't access i3file %s"%i3file
    exit(1)
DOM = options.DOM
if not len(DOM):
    print "you must enter DOM to check for"
    exit(1)

print DOM


file_ = dataio.I3File(i3file)


while file_.more():
    
    frame = file_.pop_frame()
    if 'I3SuperDST' in frame:
        p = dataclasses.I3RecoPulseSeriesMap.from_frame(frame,"I3SuperDST")
        kk = [str(sk) for sk in p.keys()]
        if DOM in kk:
            print "Found ",DOM," here:\n"
            print frame['I3EventHeader']
            
            break


file_.close()  

