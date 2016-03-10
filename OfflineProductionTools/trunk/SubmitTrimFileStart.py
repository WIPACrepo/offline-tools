#!/usr/bin/env python

from icecube import icetray, dataclasses, dataio
import sys, os
import glob
from optparse import OptionParser
from dateutil.parser import parse
import subprocess as sub



def TrimFile(file_,firstEvt,db):
    print file_,firstEvt,db
    sub.call(["python","/data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools/TrimFileStart.py","-i",file_,"-f",str(firstEvt),"-d",db])                        

    
    
            
if __name__ == '__main__':
    
    # example command
    # python /data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools/SubmitTrimFileStart.py -i infile.i3.bz2 -o outfile.i3.gz -f 100 -d
    
    # handling of command line arguments  
    parser = OptionParser()
    usage = """%prog [options]"""
    parser.set_usage(usage)
    parser.add_option("-g", "--grlfile", action="store", type="string", 
                      dest="GRLFile", help="good run list file")
    parser.add_option("-f", "--firstevtfile", action="store", type="string", 
                      dest="FirstEvtFile", help="file containing list of first good event for each run")
    parser.add_option("-o", "--outdir", action="store", type="string",  
                      dest="OutDir", help="specify main output dir if different from what is listed in GRL, useful for L3")
    parser.add_option("-d", "--updatedb", action="store_true", default=False, 
                      dest="UpdateDB", help="update DB records for trimmed file")
    
    
    # get parsed args
    (options,args) = parser.parse_args()
    
    grl_file = options.GRLFile
    fevt_file = options.FirstEvtFile
    if not (grl_file and fevt_file):
        print 'good run list file for season and file containing list of first good event for each run must be specified .... exiting'
        exit(1)
    
    out_dir = options.OutDir
    
    db = 'False'
    if options.UpdateDB: db = 'True'
    
    try:
        GRL_ = open(grl_file,'r')
        FEvt_ = open(fevt_file,'r')
    except Exception, err:
        print "Could not open %s for reading"%grl_file
        raise Exception("Error: %s "%str(err))
    
    FirstEvtDict = {}
    FirstEvt = FEvt_.readlines()
    for f in FirstEvt:
        if len(f)>1:
            try:
                f_ = f.split()
                FirstEvtDict[f_[0]] = int(f_[1])
            except:
                pass
    
    runNums = GRL_.readlines()
    
    #runNums = runNums[4:5]
    
    for r in runNums:
        
        tmp = r.split()
        
        #if tmp[0] != '120028': continue
        #if tmp[0] < '122053': continue
        
        try:
            OutputDir = tmp[-1]
            if tmp[0] in FirstEvtDict and os.path.isdir(OutputDir):
                
                if out_dir:
                    newDir = OutputDir.split("/")
                    OutputDir = os.path.join(out_dir,newDir[-2])
                
                
                files_ = glob.glob(OutputDir+"/*%s*"%tmp[0])
                #
                if len(files_) == 1 and os.path.isdir(files_[0]):
                    files_ = glob.glob(files_[0]+"/*%s*"%tmp[0])
                    
                files_ = [f for f in files_ if 'hd5' not in f and 'hdf5' not in f \
                                                and '_IT' not in f and 'SLOP' not in f and 'EHE' not in f\
                                                and 'GCD' not in f and 'root' not in f and 'txt' not in f]
                
                files_.sort()
                # ensure that you're trimming first file
                if '0000.i3.bz2' in files_[0]:
                    print tmp[0],FirstEvtDict[tmp[0]],OutputDir,files_[0]
                    TrimFile(files_[0],FirstEvtDict[tmp[0]],db)
                    
                else:
                    print files_[0],' may not be first file, skipping .....'
                
            else:
                print "Not Processing ",tmp
        
        except Exception, err:
            raise Exception("Error: %s "%str(err))
    
    
    
    