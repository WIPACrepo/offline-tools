#!/usr/bin/env python

# make a cache of chksums for PFFilt files
# source: file system

import sys, os
from os.path import expandvars, join, exists
import glob
import cPickle
from datetime import date,datetime,timedelta
from subprocess import Popen, PIPE
import datetime

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools')
from RunTools import *
from FileTools import *


print "Attempting Update @ %s"%datetime.datetime.now().isoformat().replace("T"," ")

if os.path.isfile("/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/Chksums_SubmitLock.txt"):
    f = open("/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/Chksums_SubmitLock.txt",'r')
    pid = f.readline()
    # Check if a process with this pid is still running, just printing the command w/o the ps header (so, no line if no process with PID is running)
    sub_proc = sub.Popen(['ps', '-p', str(pid), '-o', 'command='], shell=False, stdout=sub.PIPE)
    for line in sub_proc.stdout:
        # Check if the running process is still a PoleGCDCheck (is required since the PIDs are recycled)
        if 'CacheChksums_2015.py' in line:
            #print "Another instance of the Cache chksums script is running ... exiting"
            print "Another instance of the Cache chksums script is running @ %s... exiting"%datetime.datetime.now().isoformat().replace("T"," ")
            exit(0)

    print "removing stale lock file"
    os.system("rm -f /data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/Chksums_SubmitLock.txt")

with open("/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/Chksums_SubmitLock.txt",'w') as f:
    f.write(str(os.getpid()))


ChkSums = {}

ChkSumCacheFile = "/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/IC86_2015.dat"

try:
    if os.path.isfile(ChkSumCacheFile):
        ChkSumsFile = open(ChkSumCacheFile,'rb')
        ChkSums = cPickle.load(ChkSumsFile)
        ChkSumsFile.close()
    else:
        os.system("touch %s "%(ChkSumCacheFile))

except:
    print "could not open existing or new ChkSum cache file %s"%ChkSumCacheFile


current_day = date.today()
look_back = current_day + timedelta(days=-14)
dump_interval = 20
dump_count = 0

while look_back <= current_day:
    
    #print look_back
    #print InDir
   
    InDir = "/data/exp/IceCube/%s/filtered/PFFilt/%s%s"%(look_back.year,str(look_back.month).zfill(2),str(look_back.day).zfill(2))
    
    if os.access(InDir,os.R_OK):
    	
    	InFiles = glob.glob(InDir+"/PFFilt*.tar.bz2")
        InFiles.sort()
        
        for File in InFiles:
            if not File in ChkSums.keys():
                print File
                try:
                    ChkSums[File] = str(FileTools(File).md5sum())
		except Exception, err:
		    print "Error: %s "%str(err)   
 
                if dump_count>=dump_interval:
                    with open(ChkSumCacheFile,mode="r+") as f:
                        cPickle.dump(ChkSums,f)
                    dump_count = 0
    
                dump_count+=1
    
    look_back+=timedelta(days=1)
    

with open(ChkSumCacheFile,mode="r+") as f:
    cPickle.dump(ChkSums,f)

if os.path.isfile("/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/Chksums_SubmitLock.txt"):
    print "removing cache chksum submission lock file"
    os.system("rm -f /data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/Chksums_SubmitLock.txt")
