#!/usr/bin/env python

import os, sys
import StringIO
import subprocess as sub
import logging
from optparse import OptionParser
from dateutil.parser import parse
import glob
import datetime
import json
from dateutil.relativedelta import *
from math import *


try:
    from icecube.icetray import *
    from I3Tray import *
    #from icecube import icetray, dataclasses, dataio, phys_services,tpx, dst, linefit, I3Db
    from icecube import dataclasses, dataio
except Exception,err:
    print "\n*** Make sure I3Tray enviroment is enabled ...\n"
    raise Exception("Error: %s\n"%str(err))


from RunTools import *
from FileTools import *

#import MySQLdb
#sys.path.append("/net/user/i3filter/SQLServers_n_Clients/npx4/")

try:
    import SQLClient_dbs4 as dbs4
    dbs4_ = dbs4.MySQL()
    
except Exception, err:
    raise Exception("Error: %s "%str(err))


class TrimFileClass(I3PacketModule):
   def __init__(self, context):
       I3PacketModule.__init__(self, context, icetray.I3Frame.DAQ)
       self.AddOutBox('OutBox')

   def Configure(self):
       pass
   def FramePacket(self, frames):
        
        if frames[0]['I3EventHeader'].event_id>=first_evt:
                    for fr in frames:
                        self.PushFrame(fr)



def TrimFile(InFile,TrimmedFile,FirstGoodEvt):
        
        check_ = 0
        
        try:
            
            tray = I3Tray()
            
            tray.AddModule("I3Reader","readL2File", filename = InFile)
            
            tray.AddModule(TrimFileClass, 'Trim')
            
            tray.AddModule('I3Writer', 'FileWriter',
                            FileName = TrimmedFile,
                            Streams = [ I3Frame.DAQ,
                                        I3Frame.Physics]
                        )
            
            tray.AddModule("TrashCan","trash")
               
            #tray.Execute(1000)
            tray.Execute()
            
            tray.Finish()
        
        except Exception, err:
            check_ = 1
            print "Did not successfully trim file: %s "%InFile
            raise Exception("Error: %s "%str(err))
            #print "Error: %s "%str(err)
        
        finally:
            return check_
        


def UpdateDB(File):
    
    print """select * from i3filter.urlpath u
                    where name="%s" and path="%s" """%\
                    (os.path.basename(File),'file:'+os.path.dirname(File))
                    
    records_ = dbs4_.fetchall("""select * from i3filter.urlpath u
                    where name="%s" and path="%s" """%\
                    (os.path.basename(File),'file:'+os.path.dirname(File)))
    
    if len(records_):
        print """update i3filter.urlpath u
                         set md5sum="%s", size="%s", transferstate="WAITING"
                         where name="%s" and path="%s" """%\
                        (str(FileTools(File).md5sum()),str(os.path.getsize(File)),os.path.basename(File),'file:'+os.path.dirname(File))
        
        dbs4_.execute("""update i3filter.urlpath u
                         set md5sum="%s", size="%s", transferstate="WAITING"
                         where name="%s" and path="%s" """%\
                        (str(FileTools(File).md5sum()),str(os.path.getsize(File)),os.path.basename(File),'file:'+os.path.dirname(File))
                     )


if __name__ == '__main__':
    
    # example command
    # python /data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools/TrimFileStart.py 
    
    # handling of command line arguments  
    parser = OptionParser()
    usage = """%prog [options]"""
    parser.set_usage(usage)
    parser.add_option("-i", "--infile", action="store", type="string", 
                      dest="InputFile", help="Input i3 file to trim")
    parser.add_option("-o", "--outfile", action="store", type="string",  
                      dest="OutputFile", help="Output i3 file after trimming")
    parser.add_option("-f", "--firstgoodevt", action="store", type="int",  
                      dest="FirstGoodEvt", help="First good event in .i3 file, trim all before")
    parser.add_option("-d", "--updatedb", action="store", default='False', 
                      dest="UpdateDB", help="update DB records for trimmed file")
    
    # get parsed args
    (options,args) = parser.parse_args()
    
    in_file = options.InputFile
    first_evt = int(options.FirstGoodEvt)
    if not (in_file and first_evt):
        print 'both file to be trimmed and first good event_id must be specified .... exiting'
        exit(1)
    if not os.path.isfile(in_file):
        print in_file,' not accessible, please check path and permissions'
        
    out_file = options.OutputFile
    if not out_file:
        print 'no output file specified so input file will be replaced after trimming'
        out_file = in_file
    
    update_db = options.UpdateDB

    l_name = os.path.basename(in_file)
    sub.call(["cp",in_file,"./%s"%l_name])
    
    trimmed_file = "Trimmed_"+os.path.basename(l_name)
            
    trimmed_ = TrimFile(in_file,trimmed_file,first_evt)
    
    #print trimmed_
    
    if not trimmed_:
        print "mv %s %s"%(trimmed_file,out_file)
        sub.call(["mv","-f",trimmed_file,out_file])
        
    if update_db=='True':
        UpdateDB(out_file)
    
    if os.path.isfile(l_name):
        sub.call(["rm",l_name])
