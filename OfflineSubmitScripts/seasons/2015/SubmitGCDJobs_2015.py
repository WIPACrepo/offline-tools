#!/usr/bin/env python

#############################################################################
#
#       General Description:    set run status in i3filter.pre_processing_checks table of dbs4
#                               gather information from i3live, dbs2 and input PFFilt files
#
#       General Usage: python SubmitGCDJobs_2015.py [ -s startRun -e EndRun]
#
# Copyright: (C) 2014 The IceCube collaboration
#
# @file    $Id$
# @version $Revision$

# @date    05/04/2015
# @author  Oladipo Fadiran <ofadiran@icecube.wisc.edu>
#
#############################################################################

import sys, os
import glob
from optparse import OptionParser
import datetime
from dateutil.relativedelta import *
import time
import cPickle


##-----------------------------------------------------------------
## setup DB
##-----------------------------------------------------------------

try:
    import SQLClient_i3live as live
    m_live = live.MySQL()

    import SQLClient_dbs4 as dbs4
    dbs4_ = dbs4.MySQL()

except Exception, err:
    raise Exception("Error: %s "%str(err))

##-----------------------------------------------------------------
usage = "usage: %prog [options]"
parser = OptionParser(usage)

parser.add_option("-s", "--startrun", type="int", default=-1,
                  dest="STARTRUN_", help="start status update from this run")

parser.add_option("-e", "--endrun", type="int", default=-1,
                  dest="ENDRUN_", help="end status update at this run")

parser.add_option("-r", "--reproduce", action="store_true", default=False,
              dest="REPRODUCE_", help="regenerate GCD file even if already attempted")

#parser.add_option("-b", "--i3build", default='/data/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.4_IC2014-L2_V14-02-00_NewCVMFS/',
#                  dest="I3BUILD_", help="icerec build directory")
#parser.add_option("-b", "--i3build", default='/data/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.4_IC2015-L2_V15-04-01/',
#                  dest="I3BUILD_", help="icerec build directory")
#parser.add_option("-b", "--i3build", default='/data/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.4_IC2015-L2_V15-04-02/',
#                  dest="I3BUILD_", help="icerec build directory")
#parser.add_option("-b", "--i3build", default='/data/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.4_IC2015-L2_V15-04-03/',
#                  dest="I3BUILD_", help="icerec build directory")
#parser.add_option("-b", "--i3build", default='/data/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.4_IC2015-L2_V15-04-04/',
#                  dest="I3BUILD_", help="icerec build directory")
parser.add_option("-b", "--i3build", default='/data/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.4_IC2015-L2_V15-04-05/',
                  dest="I3BUILD_", help="icerec build directory")


parser.add_option("-p", "--pythonscriptdir", default='/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/',
                  dest="PythonScriptDir_", help="directory containing python scripts to be used for GCD generation and auditing")


#-----------------------------------------------------------------
# Parse cmd line args, bail out if anything is not understood
#-----------------------------------------------------------------

(options,args) = parser.parse_args()
if len(args) != 0:
    message = "Got undefined options:"
    for a in args:
        message += a
        message += " "
    parser.error(message)


##-----------------------------------------------------------------
## Check and store arguments
##-----------------------------------------------------------------

START_RUN = options.STARTRUN_

END_RUN = options.ENDRUN_

REPRODUCE = options.REPRODUCE_

I3BUILD = options.I3BUILD_
if os.access(I3BUILD,os.R_OK) == False:
    raise RuntimeError("cannot access I3Build directory %s for reading!"%I3BUILD)

PYTHONSCRIPTDIR = options.PythonScriptDir_
if os.access(PYTHONSCRIPTDIR,os.R_OK) == False:
    raise RuntimeError("cannot access directory containing python scripts"%PYTHONSCRIPT)

GRLInfo_ = dbs4_.fetchall("""SELECT r.run_id,r.tStart,g.production_version,g.snapshot_id
                          FROM i3filter.grl_snapshot_info g join i3filter.run_info_summary r
                          on r.run_id=g.run_id
                          where ((not g.GCDCheck and not g.BadDOMsCheck and not g.submitted) or %s)
                          and g.run_id>=%s and g.run_id<=%s"""%(REPRODUCE,START_RUN,END_RUN),UseDict=True)

if not len(GRLInfo_):
    print "no runs meet input criteria for GCD generation ... exiting"
    exit(0)


GRLInfo = {}
for g in GRLInfo_:
    GRLInfo[g['run_id']] = [g['tStart'],g['production_version'],g['snapshot_id']]

RunNums = GRLInfo.keys()
RunNums.sort()

for r in RunNums:

    #if r not in  [123615,123616] : continue
    
    print r, GRLInfo[r]

    StartDay = GRLInfo[r][0]
    PV = GRLInfo[r][1]
    SId = GRLInfo[r][2]
    
    sY = str(StartDay.year)
    sM = str(StartDay.month).zfill(2)
    sD = str(StartDay.day).zfill(2)
    
    Clog = "/data/exp/IceCube/%s/filtered/level2/OfflinePreChecks/run_logs/condor_logs/%s%s/"%(sY,sM,sD)
    Cerr = "/data/exp/IceCube/%s/filtered/level2/OfflinePreChecks/run_logs/condor_err/%s%s/"%(sY,sM,sD)
    Olog = "/data/exp/IceCube/%s/filtered/level2/OfflinePreChecks/run_logs/logs/%s%s/"%(sY,sM,sD)
    
    try:
        if not os.path.exists(Clog):
            os.mkdir(Clog)
        if not os.path.exists(Cerr):
            os.mkdir(Cerr)
        if not os.path.exists(Olog):
            os.mkdir(Olog)
    except Exception, err:
        raise Exception("Error: %s "%str(err))
    
    
    SUBMITFILE = open("submit_GCD_2015.condor","w")
    SUBMITFILE.write("Universe = vanilla ")
    SUBMITFILE.write('\nExecutable = %s/./env-shell.sh'%I3BUILD)
    SUBMITFILE.write("\narguments =  python -u %s/GCDGenerator_2015.py %s %s %s "%(PYTHONSCRIPTDIR,r,PV,SId))
    SUBMITFILE.write("\nLog = %s/Run00%s_%s_%s.log"%(Clog,str(r),PV,SId))
    SUBMITFILE.write("\nError = %s/Run00%s_%s_%s.err"%(Cerr,str(r),PV,SId))
    SUBMITFILE.write("\nOutput = %s/Run00%s_%s_%s.out"%(Olog,str(r),PV,SId))
    SUBMITFILE.write("\nNotification = Never")
    #SUBMITFILE.write("\nRequestMemory = 4000")
    #SUBMITFILE.write("\nRequirements = TARGET.TotalCpus == 16")
    SUBMITFILE.write("\nRequirements = TARGET.TotalCpus == 32")
    SUBMITFILE.write("\npriority = 15")
    #SUBMITFILE.write("\n+IsTestQueue = TRUE")
    #SUBMITFILE.write("\nrequirements = TARGET.IsTestQueue")
    SUBMITFILE.write('\ngetenv = True')
    SUBMITFILE.write("\nQueue")
    SUBMITFILE.close()
    ##
    os.system("condor_submit submit_GCD_2015.condor")
    
