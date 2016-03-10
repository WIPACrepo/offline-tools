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



def UpdateGapsFile(InFile):
        
        try:
                
            l_name = os.path.basename(InFile)

            sub.call(["cp",InFile,"./%s"%l_name])
    
            tFile = l_name.replace('.i3.bz2','_gaps.txt')
    
            out_file = os.path.join(os.path.dirname(in_file),tFile)
            
                
            with open(tFile,'w') as tf:
            
                file_ =  dataio.I3File(InFile)
                frame = file_.pop_frame()
                #print frame['I3EventHeader'].run_id
                tf.write("Run: %s\n"%frame['I3EventHeader'].run_id)
                #print frame['I3EventHeader'].event_id
                sTime = frame['I3EventHeader'].start_time
                #print sTime.utc_year
                #print sTime.utc_daq_time
                tf.write("First Event of File: %s %s %s\n"%(frame['I3EventHeader'].event_id,sTime.utc_year,sTime.utc_daq_time))
                
                while file_.more(): frame = file_.pop_frame()
                
                #print frame['I3EventHeader'].event_id
                eTime = frame['I3EventHeader'].end_time
                #print eTime.utc_year
                #print eTime.utc_daq_time
                tf.write("First Event of File: %s %s %s\n"%(frame['I3EventHeader'].event_id,eTime.utc_year,eTime.utc_daq_time))
                
                lTime = (eTime - sTime)/1000000000
                
                tf.write("File Livetime: %s\n"%(lTime))
                
            print "mv %s %s"%(tFile,out_file)
            sub.call(["mv","-f",tFile,out_file])
            
            if os.path.isfile(l_name):
                print "rm %s"%l_name
                sub.call(["rm",l_name])
            
        
        except Exception, err:
            out_file = ''
            print "Did not successfully update file: %s "%tFile
            raise Exception("Error: %s "%str(err))
            #print "Error: %s "%str(err)
        
        finally:
            return out_file
        


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
    # python /data/user/i3filter/IC86_OfflineProcessing/OfflineProductionTools/UpdateGapsFile.py 
    
    # handling of command line arguments  
    parser = OptionParser()
    usage = """%prog [options]"""
    parser.set_usage(usage)
    parser.add_option("-i", "--infile", action="store", type="string", 
                      dest="InputFile", help="Input i3 file to trim")
    parser.add_option("-d", "--updatedb", action="store", default='False', 
                      dest="UpdateDB", help="update DB records for trimmed file")
    
    # get parsed args
    (options,args) = parser.parse_args()
    
    in_file = options.InputFile
    if not os.path.isfile(in_file):
        print in_file,' not accessible, please check path and permissions'
        exit(1)

    update_db = options.UpdateDB
            
    out_file = UpdateGapsFile(in_file)

    if os.path.isfile(out_file)and update_db=='True':
        UpdateDB(out_file)

