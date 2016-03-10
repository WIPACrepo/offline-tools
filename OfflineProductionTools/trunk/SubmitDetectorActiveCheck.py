#!/usr/bin/env python

from icecube import icetray, dataclasses, dataio
import sys, os
import glob
import subprocess as sub

GRL_2011 = open('/data/exp/IceCube/2011/filtered/level2/IC86_2011_GoodRunInfo.txt')
GRL_2012 = open('/data/exp/IceCube/2012/filtered/level2/IC86_2012_GoodRunInfo.txt')
runNums = GRL_2012.readlines()


#count = 0

CorrectionsFile = 'IC86_2012_Corrections.txt'
#of_ = open(CorrectionsFile,'w')
FirstGoodEvtFile = 'IC86_2012_FirstGoodEvt.txt'
of_ = open(FirstGoodEvtFile,'w')
of_.write("RunNum\tEventId\n")
of_.close()

#runNums = runNums[0:5]

with open('checkDetectorActive.log','a') as outfile:

    for r in runNums:
        
        tmp = r.split()
        if len(tmp)>=8:
            try:
                #print 'runNum ',tmp[0]
                
                if os.path.isdir(tmp[7]):
                    L2Files = glob.glob(tmp[7]+"/Level2*%s*"%tmp[0])
                    gcdfile = [g for g in L2Files if 'GCD' in g]
                    infile = [i for i in L2Files if 'GCD' not in i and '_IT' not in i and 'SLOP' not in i and 'EHE' not in i and 'root' not in i and 'txt' not in i and 'Missing' not in i]
                    if len(gcdfile) and len(infile):
                        gcdfile.sort()
                        gcdfile = gcdfile[-1]
                        infile.sort()
                        infile = infile[0]
                        outfile.write("%s, %s \n"%(gcdfile,infile))
                        #sub.call(["python","/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools/checkFullDetectorActive.py","-i","%s"%(infile),"-g","%s"%(gcdfile),"-o",FirstGoodEvtFile])
                        sub.call(["python","/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools/checkFullDetectorActive_1.py","-i","%s"%(infile),"-g","%s"%(gcdfile),"-o",FirstGoodEvtFile])
                        #if count>=3: print tyr 
                #count+=1
            except Exception, err:
                print "**Check Run %s **"%r
                raise Exception("Error: %s "%str(err))
            




