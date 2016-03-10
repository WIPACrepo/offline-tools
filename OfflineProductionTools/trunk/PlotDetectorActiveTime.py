
#!/usr/bin/env python

#from icecube import icetray, dataclasses, dataio
import sys, os
import numpy as np
import scipy.stats as stats
import pylab as pl

#import glob
#import subprocess as sub

#logs = open('nohup_checkDetectorActive.log')
logs = open('nohup_checkDetectorActive_WholeDetector.log')

l =logs.readlines()


diff_ = []
evts_ = []
for ll in l:
    tmp = ll.split()
    #if len(tmp) == 10:
    if len(tmp) == 12:
        #print tmp,tmp[-1],tmp[-2]
        
        #evts_.append(float(tmp[-1]))
        #diff_.append(round(float(tmp[-2])/1000000,2))

        evts_.append(float(tmp[-2]))
        #evts_.append(float(tmp[-3]))
        diff_.append(round(float(tmp[-4])/1000000,2))


#print evts_
print sum(evts_)/len(evts_)
print min(evts_)
print max(evts_)
#print diff_
print sum(diff_)/len(diff_)
print min(diff_)
print max(diff_)
#
#print [d for d in diff_ if d>=1000]

#diff_.sort()
#fit = stats.norm.pdf(diff_, np.mean(diff_), np.std(diff_))  #this is a fitting indeed
##pl.plot(diff_,fit,'-o')
##pl.hist(diff_,20,normed=True)
#pl.hist(diff_,100,normed=True)
#pl.xlabel('Elapsed Time Before Full Active Detector (ms)',fontsize=16)
#pl.savefig('DetectorActiveElapsedTime_WholeDetector.png')


evts_ = [e for e in evts_ if e>0]

evts_.sort()
fit = stats.norm.pdf(evts_, np.mean(evts_), np.std(evts_))  #this is a fitting indeed
#pl.hist(evts_,100,normed=True,histtype='bar')
#pl.hist(evts_,range(1,int(max(evts_))+10),normed=True,histtype='bar')
pl.hist(evts_,range(int(max(evts_))+10),histtype='bar')
pl.xlabel('# of Events Before Full Active Detector',fontsize=16)
pl.savefig('DetectorActiveEvtCount_WholeDetector.png')
pl.show()
