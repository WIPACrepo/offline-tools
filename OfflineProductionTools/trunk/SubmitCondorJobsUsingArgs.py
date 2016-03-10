#!/usr/bin/env python

#############################################################################
#
#  	General Description: 	submit condor_jobs usng arguments stored in pickled file
#                           monitors existing jobs in queue and submits more when needed
#
#	General Usage: python SubmitCondorJobsUsingArgs SubmitArgs.dat
#
# Copyright: (C) 2012 The IceCube collaboration
#
# @file    $Id$
# @version $Revision$

# @date    05/20/2013
# @author  Oladipo Fadiran <ofadiran@icecube.wisc.edu>
#
#############################################################################

import sys, os
import glob
from optparse import OptionParser
import datetime
import time
from dateutil.relativedelta import *
import time
import cPickle
from subprocess import Popen, PIPE


usage = "usage: %prog [options]"
parser = OptionParser(usage)

parser.add_option("-s", "--submitargs",
                  dest="SUBMITARGS_", help="pickled files containing args. to be submitted")

parser.add_option("-q", "--queued", type="int", default=3000,
                  dest="QUEUED_", help="number to be maintained in condor queue")

parser.add_option("-w", "--workingdir", default='/net/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2012/',
                  dest="WDIR_", help="working directory")

parser.add_option("-b", "--i3build", default='/net/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.0_amd64_IC2012-L2_V13-01-00_EHE_ReProcess',
                  dest="I3BUILD_", help="icerec build directory")

parser.add_option("-p", "--pythonscriptdir", default='/net/user/i3filter/IC86_OfflineProcessing/icerec/IC2012-L2_V13-01-00_EHE_ReProcess/filter-2012/python/',
                  dest="PythonScriptDir_", help="directory containing python scripts to be used for processing")


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

ARGS_FILE = options.SUBMITARGS_

QUEUED = options.QUEUED_

W_DIR = options.WDIR_

I3BUILD = options.I3BUILD_
if os.access(I3BUILD,os.R_OK) == False:
    raise RuntimeError("cannot access I3Build directory %s for reading!"%I3BUILD)

PYTHONSCRIPTDIR = options.PythonScriptDir_
#print PYTHONSCRIPTDIR
if os.access(PYTHONSCRIPTDIR,os.R_OK) == False:
	raise RuntimeError("cannot access directory containing python scripts"%PYTHONSCRIPTDIR)


print "\n========================"

if os.path.isfile("%s/SubmitLock.txt"%W_DIR):
    f = open("%s/SubmitLock.txt"%W_DIR,'r')
    l = f.readline()
    sub_proc = Popen(['ps', 'aux'], shell=False, stdout=PIPE)
    for line in sub_proc.stdout:
        if str(l) in line:
            print "Another instance of the submission script is running ... exiting"
            exit(0)
    print "removing stale lock file"
    os.system("rm -f %s/SubmitLock.txt"%W_DIR)

with open("%s/SubmitLock.txt"%W_DIR,'w') as f:
    f.write(str(os.getpid()))

print "Starting submission, attempting job submission at ", datetime.datetime.now()

print "Attempting to maintain %s jobs in queue"%str(QUEUED)

SubmitArgs = []
try:
    SubmitArgs = cPickle.load(open(ARGS_FILE,"r"))
except:
    print "failed to open file containing condor args."
    if os.path.isfile("%s/SubmitLock.txt"%W_DIR):
        print "removing submission lock file"
        os.system("rm -f %s/SubmitLock.txt"%W_DIR)
    exit(1)
    
if not len(SubmitArgs):
    print "No arguments for job(s) to be submitted in %s"%ARGS_FILE
    if os.path.isfile("%s/SubmitLock.txt"%W_DIR):
        print "removing submission lock file"
        os.system("rm -f %s/SubmitLock.txt"%W_DIR)
    exit(1)
    
print len(SubmitArgs)," number of arguments in master file before submission attempt"
  
try:    
    sub_proc = Popen(['condor_q', 'i3filter'], shell=False, stdout=PIPE)
    RunningJobs = len (sub_proc.stdout.readlines()) - 6
except:
    RunningJobs = 0

print RunningJobs, " currently queued/running job(s)"


Jobs2Submit = QUEUED - RunningJobs


Jobs2Submit = min(Jobs2Submit,len(SubmitArgs))

print "*** submitting %s new job(s) ***"%str(Jobs2Submit)

s = 0
while s < Jobs2Submit:
    
    [GCDFile, f, Out_EHE, r, sRun, Clog, Cerr, Olog] = SubmitArgs.pop(0)
    
    SUBMITFILE = open("%s/submit_EHE.condor"%W_DIR,"w")
    SUBMITFILE.write("Universe = vanilla ")
    SUBMITFILE.write('\nExecutable = %s/./env-shell.sh'%I3BUILD)
    SUBMITFILE.write("\narguments =  python -u %s/ReProcess_EHE.py %s %s %s"%(PYTHONSCRIPTDIR,GCDFile,f,Out_EHE))
    SUBMITFILE.write("\nLog = %s/Run00%s_%s"%(Clog,str(r),sRun+".log"))
    SUBMITFILE.write("\nError = %s/Run00%s_%s"%(Cerr,str(r),sRun+".err"))
    SUBMITFILE.write("\nOutput = %s/Run00%s_%s"%(Olog,str(r),sRun+".out"))
    SUBMITFILE.write("\nNotification = Never")
    SUBMITFILE.write("\npriority = 15")
    #SUBMITFILE.write("\n+IsTestQueue = TRUE")
    #SUBMITFILE.write("\nrequirements = TARGET.IsTestQueue")
    SUBMITFILE.write("\nrequirements = TARGET.TotalCpus > 15")
    SUBMITFILE.write('\ngetenv = True')
    #SUBMITFILE.write('\nhold = True')
    SUBMITFILE.write("\nQueue")
    SUBMITFILE.close()
    	    
    os.system("condor_submit %s/submit_EHE.condor"%W_DIR)
                
    
    #time.sleep(1)

    s+=1
    
#os.system("condor_release i3filter")

print len(SubmitArgs), " job(s) left in master file after submission"

cPickle.dump(SubmitArgs,open(ARGS_FILE,"w"))


if os.path.isfile("%s/SubmitLock.txt"%W_DIR):
    print "removing submission lock file"
    os.system("rm -f %s/SubmitLock.txt"%W_DIR)
    
print "========================"
