#!/usr/bin/env python

from icecube import icetray, dataclasses, dataio
import sys, os

from optparse import OptionParser


# example command
# python FindObjects.py -i Level2_IC86.2014_data_Run00124840_Subrun00000000.i3.bz2 -o object1 -o object2

# handling of command line arguments  
parser = OptionParser()
usage = """%prog [options]"""
parser.set_usage(usage)
parser.add_option("-i", "--infile", action="store", type="string", default="", 
                  dest="InputFile", help="Input i3 file to search")
parser.add_option("-o", "--objects", action="append", type="string", default=[], 
                  dest="Objects", help="Objects to search for")
parser.add_option("-f", "--findfirst", action="store_true", default=False,
                  dest="OnlyFirst", help="Find only the first occurence")


# get parsed args
(options,args) = parser.parse_args()

i3file = options.InputFile
if not os.path.isfile(i3file):
    print "can't access i3file %s"%i3file
    exit(1)
objects = options.Objects
if not len(objects):
    print "you must enter at least 1 object name to search for"
    exit(1)
onlyfirst = options.OnlyFirst



Results = {}
for ob in objects: Results[ob] = 0


file_ = dataio.I3File(i3file)

frameCount = 1
if onlyfirst:
    while file_.more():
        frame = file_.pop_frame()
        for k in Results.keys():
            if frame.Has(k) and not Results[k]: Results[k] = frameCount
            
        if all(Results.values()) : break
        
        frameCount+=1

    print "\nShowing *only* frame No. of first occurrence\nObjectName\tFirstFrameOccurrence"
    for k in Results.keys():
        if Results[k]:print k,"\t",Results[k]
        else:print k,"\t","Not found"
    print "\n"

else:
    while file_.more():
        frame = file_.pop_frame()
        for k in Results.keys():
            if frame.Has(k) : Results[k]+=1
        
        frameCount+=1    

    print "\nShowing No. of times an object is found in frame, 0 when the object is not found\nTotal No. of Frames: ",frameCount
    print "ObjectName\tNumberOfOccurrences"
    for k in Results.keys():
        print k,"\t",Results[k]
    print "\n"
    
    

file_.close()        


