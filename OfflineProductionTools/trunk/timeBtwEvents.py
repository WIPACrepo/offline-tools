            
#!/usr/bin/env python

from icecube import icetray, dataclasses, dataio
import sys, os

from optparse import OptionParser

import sys, os
import numpy as np
import scipy.stats as stats
import pylab as pl


def checkTimes(i3file):
    
    times_ = []
    evtId_ = []
    diffTimes_ = []
    
    file_ = dataio.I3File(str(i3file))
    
    while file_.more():
        
        #frame = file_.pop_daq()
        frame = file_.pop_frame()
        
        if frame and 'I3EventHeader' in frame:
            #print frame['I3EventHeader'].event_id,frame['I3EventHeader'].start_time
            times_.append(frame['I3EventHeader'].start_time)
            evtId_.append(frame['I3EventHeader'].event_id)

    #print times_
    for t_ in range(1,len(times_)):
        print evtId_[t_],times_[t_],times_[t_]-times_[t_-1]
        diffTimes_.append(times_[t_]-times_[t_-1])

    pl.plot(evtId_[1:],diffTimes_)
    pl.savefig('test_.png')
    pl.show('test_.png')


if __name__ == '__main__':
    # example command
    ## python /data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools/checkFullDetectorActive.py -i Level2File.i3.bz2 -g GCDFile.i3.gz
    
    # handling of command line arguments  
    parser = OptionParser()
    usage = """%prog [options]"""
    parser.set_usage(usage)
    parser.add_option("-i", "--infile", action="store", type="string", default="", 
                      dest="InputFile", help="Input i3 file to search")
    #parser.add_option("-g", "--gcdfile", action="store", type="string", default="", 
    #                  dest="GCDFile", help="Input gcd file to supply BDL")
    #parser.add_option("-o", "--outlogfile", action="store", type="string", default="outlogfile.txt", 
    #                  dest="OutFile", help="file to write check results")
    #
    ## get parsed args
    (options,args) = parser.parse_args()
    #
    i3file = options.InputFile
    #gcdfile = options.GCDFile
    #outfile = options.OutFile
    #
    #try:
    #    of_ = open(outfile,'a')
    #except Exception, err:
    #    print "Could not open %s for appending"%outfile
    #    raise Exception("Error: %s "%str(err))
    #
    checkTimes(i3file)
    #
    #of_.close()
    
