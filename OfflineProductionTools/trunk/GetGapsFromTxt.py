#!/usr/bin/env python

import os, sys
import glob

from icecube import icetray,dataclasses

#import subprocess as sub
#import time
#import datetime
#
#from RunTools import *
#from FileTools import *
#from DbTools import *

# Spits out time gaps in seconds between subruns
# example use python GetGapsFromTxt.py RunNum InDir

if len(sys.argv)!=3:
    print "need to enter both RunNum and InDir arguments e.g. python GetGapsFromTxt.py 125553 /data/exp/IceCube/2014/filtered/level2/1112/Run00125553"
    exit(1)

try:
    RunNum = int(sys.argv[1])
except Exception, err:
    print "need to enter RunNumber as first argument"
    raise Exception("Error: %s "%str(err))

if not os.path.isdir(sys.argv[2]):
    print "need to enter Dir. containing .txt gaps files as second argument"
    exit(1)

tFiles = glob.glob(sys.argv[2]+"/*%s*_gaps.txt"%(RunNum))

tFiles.sort()

#F = open(tFiles[0])

#print F.readlines()

print "\t\t\tFile1 \t\t\t\t\t\t\t\tFile2 \t\t\t\t\tGap(secs) \tFlagged"
for tf in range(len(tFiles)-1):
    #print os.path.basename(tFiles[tf]),os.path.basename(tFiles[tf+1])
    F1 = open(tFiles[tf])
    L1 = [l for l in F1.readlines() if "Last Event" in l][0]
    L1 = L1.split()
    #print L1
    lTime = dataclasses.I3Time(int(L1[-2]),int(L1[-1]))
    
    F2 = open(tFiles[tf+1])
    L2 = [l for l in F2.readlines() if "First Event" in l][0]
    L2 = L2.split()
    #print L2
    fTime = dataclasses.I3Time(int(L2[-2]),int(L2[-1]))
    
    #print lTime
    #print fTime
    
    if (fTime - lTime<0) or (fTime - lTime)/10**9>0.05:
        print os.path.basename(tFiles[tf]),"\t",os.path.basename(tFiles[tf+1]),"\t",(fTime - lTime)/10**9,"\t**Attention**"
    else:
        print os.path.basename(tFiles[tf]),"\t",os.path.basename(tFiles[tf+1]),"\t",(fTime - lTime)/10**9
    
    #break
