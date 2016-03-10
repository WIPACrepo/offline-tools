            
#!/usr/bin/env python

from icecube import icetray, dataclasses, dataio
import sys, os

from optparse import OptionParser


def checkTimes(gcdfile,i3file,of_):

    runNum = gcdfile[gcdfile.find('Run00')+5 : gcdfile.find('Run00')+11]


    if not os.path.isfile(gcdfile):
        print "can't access gcdfile %s"%gcdfile
        exit(1)
    
    if not os.path.isfile(i3file):
        print "can't access i3file %s"%i3file
        exit(1)
    
    f = dataio.I3File(str(gcdfile))
    BDL = None
    while f.more():
        GCD = f.pop_frame()
        if GCD.Has("BadDomsList") :
            BDL = GCD["BadDomsList"]
            break
    
    if not BDL:
        print "No BadDomsList object in GCD file ... exiting"
        return(1)

   
    # make default configuration
    #detConf = {}    
    #for s in range(1,87):
    #    detConf[s] = range(1,65)
    #
    ## remove DOMs in BDL
    #for b in BDL:
    #    detConf[b.string].pop(detConf[b.string].index(b.om))
    #
    #
    ## count number of strings with at least 1 active non-IceTop DOM
    #ActiveStrings = [k for k in detConf.keys() if len(set(detConf[k]).difference([61,62,63,64]))]
    ##print ActiveStrings,len(ActiveStrings)
    
    inIce = {}
    for s in range(1,87):
        inIce[s] = range(1,61)
    for b in BDL:
        if b.om<=60: inIce[b.string].pop(inIce[b.string].index(b.om))
    ActiveInIceStrings = [k for k in inIce.keys() if len(inIce[k])]
    #print len(ActiveInIceStrings)
    #print inIce.keys(),len(inIce)
    #print ActiveInIceStrings,len(ActiveInIceStrings)
    
    
    iceTop = {}
    for s in range(1,82):
        iceTop[s] = range(61,65)
    for b in BDL:
        if b.om>60: iceTop[b.string].pop(iceTop[b.string].index(b.om))
    ActiveIceTopStrings = [k for k in iceTop.keys() if len(iceTop[k])]
    #print ActiveIceTopStrings
    #print iceTop.keys(),len(iceTop)
    #print ActiveIceTopStrings,len(ActiveIceTopStrings)

    
    file_ = dataio.I3File(i3file)
    
    count = 0
    stringList = []
    stringInIceList = []
   
    while file_.more():
        frame = file_.pop_daq()
        if 'I3SuperDST' in frame:
            firstEventTime = frame['I3EventHeader'].start_time
            #print firstEventTime
            file_.rewind()
            break

    
    idoms_ = []
    tdoms_ = []
    bad_Events = []
    
    lastInIceString = 0
    lastIceTopStation = 0
    
    while file_.more():
        
        frame = file_.pop_daq()
        
        #if frame and 'I3SuperDST' in frame and 'InIceRawData' in frame:
        if frame:
            doms_ = []
            if 'I3SuperDST' in frame:   
                p = dataclasses.I3RecoPulseSeriesMap.from_frame(frame,"I3SuperDST")
                doms_.extend(p.keys())
            if 'InIceRawData' in frame:
                doms_.extend(frame['InIceRawData'].keys())
            if 'IceTopRawData' in frame:
                doms_.extend(frame['IceTopRawData'].keys())

            
            idoms_.extend([sk.string for sk in doms_ if sk.om<=60])
            tdoms_.extend([sk.string for sk in doms_ if sk.om>60])
            
            #print "\n==========="
            #print len(set(ActiveInIceStrings) - set(idoms_)), set(idoms_)
            #print len(set(ActiveIceTopStrings) - set(tdoms_)), set(tdoms_),count
            #print "===========\n"
            
            #print "\n==========="
            #ii = list(set(idoms_))
            #ii.sort()
            #print ii,len(ii)
            #tt = list(set(tdoms_))
            #tt.sort()
            #print tt,len(tt)
            #print "===========\n"
            
            if len(set(ActiveInIceStrings) - set(idoms_)) == 1: lastInIceString = list(set(ActiveInIceStrings) - set(idoms_))[0]
            if len(set(ActiveIceTopStrings) - set(tdoms_)) == 1: lastIceTopStation = list(set(ActiveIceTopStrings) - set(tdoms_))[0]
            
            
            if not len(set(ActiveInIceStrings) - set(idoms_)) and not len(set(ActiveIceTopStrings) - set(tdoms_)) :
                fullDetectorEventTime = frame['I3EventHeader'].start_time
                print runNum,' : ',firstEventTime,fullDetectorEventTime,fullDetectorEventTime - firstEventTime, count,lastInIceString,lastIceTopStation
                #print runNum,' : ',firstEventTime,fullDetectorEventTime,fullDetectorEventTime - firstEventTime, count,lastInIceString
                
                print frame['I3EventHeader'].run_id,frame['I3EventHeader'].event_id
                of_.write("%s\t%s\n"%(frame['I3EventHeader'].run_id,frame['I3EventHeader'].event_id))
                break
            
            if count>10000:
                bad_Events = []
                print runNum,' : Taking too long to get full detector, probably something wrong with run or incomplete bad DOMs list ... *CHECK RUN* '
                of_.write("%s:\t taking too long to get full detector, probably something wrong with run or incomplete bad DOMs list ... *CHECK RUN*\n"%runNum)
                break
            
            #bad_Events.append([frame['I3EventHeader'].run_id,frame['I3EventHeader'].event_id])
            
           

        count+=1
    
    
        
    #if len(bad_Events):
    #    for b in bad_Events:
    #        of_.write("%s\t%s\n"%(b[0],b[1]))
    #else:
    #    of_.write("\n%s : taking too long to get full detector, probably something wrong with run or incomplete bad DOMs list ... *CHECK RUN*\n"%runNum)
    #
    
    file_.close()
    of_.write("\n")
    


if __name__ == '__main__':
    # example command
    # python /data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools/checkFullDetectorActive.py -i Level2File.i3.bz2 -g GCDFile.i3.gz
    
    # handling of command line arguments  
    parser = OptionParser()
    usage = """%prog [options]"""
    parser.set_usage(usage)
    parser.add_option("-i", "--infile", action="store", type="string", default="", 
                      dest="InputFile", help="Input i3 file to search")
    parser.add_option("-g", "--gcdfile", action="store", type="string", default="", 
                      dest="GCDFile", help="Input gcd file to supply BDL")
    parser.add_option("-o", "--outlogfile", action="store", type="string", default="outlogfile.txt", 
                      dest="OutFile", help="file to write check results")
    
    # get parsed args
    (options,args) = parser.parse_args()
    
    i3file = options.InputFile
    gcdfile = options.GCDFile
    outfile = options.OutFile
    
    try:
        of_ = open(outfile,'a')
    except Exception, err:
        print "Could not open %s for appending"%outfile
        raise Exception("Error: %s "%str(err))
    
    checkTimes(gcdfile,i3file,of_)
    
    of_.close()
    
