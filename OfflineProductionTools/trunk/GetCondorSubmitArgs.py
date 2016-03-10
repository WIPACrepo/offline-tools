#!/usr/bin/env python

#############################################################################
#
#  	General Description: 	Get list of arguments for "manual" condor submission of very many processes
#							usually used for re-processing without using the iceprod framework
#							Arguments are required to have log, err, out and list of processing arguments
#
#	General Usage: python GetCondorSubmitArgs ListOfDirectories 
#
# Copyright: (C) 2014 The IceCube collaboration
#
# @file    $Id$
# @version $Revision$

# @date    05/01/2014
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

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-L', '--dirList', nargs='+', type=str)
parser.add_argument('-p', '--infilePattern', default='Level*.i3.bz2' , type=str)
parser.add_argument('-e', '--envFile', default='/net/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.0_amd64_IC2013-L2_V13-06-00/./env-shell.sh' , type=str)
parser.add_argument('-x', '--executableFile', default='/data/ana/Cscd/level3a/exp/submitter/UpdateObjectNames/ChangeNames.py' , type=str)
parser.add_argument('-a', '--otherArgs', nargs='+', type=str)

InputList = vars(parser.parse_args())['dirList']

FilePattern = vars(parser.parse_args())['infilePattern']

EnvFile = vars(parser.parse_args())['envFile']

ExecutableFile = {'python':vars(parser.parse_args())['executableFile']}


otherArgs = vars(parser.parse_args())['otherArgs']

if len(otherArgs)%2:
	print "arguments must be supplied in pairs"
	exit(1)
	


#OutList = []
#
#

InputList = InputList[0:1]

for i in InputList:
	#print os.path.join(i,"condor_err")
#	#if not os.path.isdir(os.path.join(i,"condor_err")):
#	#	os.mkdir(os.path.join(i,"condor_err"))
#	#if not os.path.isdir(os.path.join(i,"condor_log")):
#	#	os.mkdir(os.path.join(i,"condor_log"))
#	#if not os.path.isdir(os.path.join(i,"condor_out")):
#	#	os.mkdir(os.path.join(i,"condor_out"))
#
#	
#	a = [{'Cerr':}]
	#print i+"/"+FilePattern
	fileList = glob.glob(i+"/"+FilePattern)
	
	fileList.sort()
	fileList = fileList[0:1]
	for f in fileList:
		errFile = os.path.join(os.path.join(i,"condor_err"),os.path.basename(f)+".err")
		logFile = os.path.join(os.path.join(i,"condor_log"),os.path.basename(f)+".log")
		outFile = os.path.join(os.path.join(i,"condor_out"),os.path.basename(f)+".out")
		

		aa = [{'Cerr':errFile},{'Clog':logFile},{'Cout':outFile},{'':f}]
		
		print "%s %s %s "%(EnvFile,ExecutableFile.keys()[0],ExecutableFile[ExecutableFile.keys()[0]])

#print EnvFile
#print ExecutableFile.keys()[0],ExecutableFile.items()[0]

#usage = "usage: %prog [options]"
#parser = OptionParser(usage)
#
#
#def splitList(option, opt, value, parser):
#	print optio.INPUTDIRS_
#	#setattr(parser.values, option.dest, value.split(''))
#
#parser.add_option("-d", "--inputdirs", type="string", default='',
#                  dest="INPUTDIRS_",
#				  action='callback',
#				  callback=splitList,
#				  help="list of directories containing input arguments")
#
##parser.add_option("-s", "--startrun", type="int", default=0,
##                  dest="STARTRUN_", help="start status update from this run")
#
##-----------------------------------------------------------------
## Parse cmd line args, bail out if anything is not understood
##-----------------------------------------------------------------
#
#(options,args) = parser.parse_args()
#if len(args) != 0:
#    message = "Got undefined options:"
#    for a in args:
#        message += a
#        message += " "
#    parser.error(message)
#    
#
###-----------------------------------------------------------------
### Check and store arguments
###-----------------------------------------------------------------
#
#opts,args = parser.parse_args()
#
##INPUTDIRS = options.INPUTDIRS_
#
#
##print INPUTDIRS
#
#raise "*"
#
#START_RUN = options.STARTRUN_
#
#END_RUN = options.ENDRUN_
#
#I3BUILD = options.I3BUILD_
#if os.access(I3BUILD,os.R_OK) == False:
#    raise RuntimeError("cannot access I3Build directory %s for reading!"%I3BUILD)
#
#PYTHONSCRIPTDIR = options.PythonScriptDir_
##print PYTHONSCRIPTDIR
#if os.access(PYTHONSCRIPTDIR,os.R_OK) == False:
#	raise RuntimeError("cannot access directory containing python scripts"%PYTHONSCRIPTDIR)
#
## add range of specified runs when specified in arguments
#Runs = []
#Runs = range(START_RUN,END_RUN+1)
#
#
#if not len(Runs):
#    print "No runs to process, check input arguments"
#    exit(0)
#
#RunStr = ""
#for r_ in Runs:
#	RunStr += str(r_)+","
#RunStr = RunStr[:-1]	# remove trailing "," from string
#
#FromLiveDb = m_live.fetchall("""SELECT runNumber,tStart FROM live.livedata_run where runNumber in (%s) """%RunStr)
#live_info = {}
#for t in FromLiveDb:
#    live_info[t[0]] = [t[1]]
#
#runs = live_info.keys()
#
#runs.sort()
#
#SubmitArgs = []
#
#
#for r in runs:
#	
#	#print r
#
#	StartDay = live_info[r][0]
#
#	sY = str(StartDay.year)
#	sM = str(StartDay.month).zfill(2)
#	sD = str(StartDay.day).zfill(2)
#
#	InDir = "/data/exp/IceCube/%s/filtered/level2/%s%s/"%(sY,sM,sD)
#    
#	InFiles = glob.glob(InDir+"*%s*"%r)
#    
#	GCDFile = [g for g in InFiles if g.find("GCD")>=0]
#	if len(GCDFile):
#		GCDFile = GCDFile[0]
#        
#		L2Files = [l for l in InFiles if l.find("GCD")<0 and l.find("root")<0 and l.find("log")<0 and
#                                          l.find("_EHE")<0 and l.find("_IT")<0 and l.find("txt")<0]
#
#        if len(L2Files):
#			L2Files.sort()
#			for f in L2Files:
#				sRun = str(int(f[f.find("Subrun")+6:f.find(".i3.bz2")]))
#				#print "submitting for: ",f
#				Out_EHE = (f.replace("Level2","Level2a")).replace(".i3.bz2","_EHE.i3.bz2")
#
#				Clog = "/data/exp/IceCube/%s/filtered/level2/IC86_2_EHE_Reprocess_Logs/condor_logs/%s%s/"%(sY,sM,sD)
#				Cerr = "/data/exp/IceCube/%s/filtered/level2/IC86_2_EHE_Reprocess_Logs/condor_err/%s%s/"%(sY,sM,sD)
#				Olog = "/data/exp/IceCube/%s/filtered/level2/IC86_2_EHE_Reprocess_Logs/logs/%s%s/"%(sY,sM,sD)
#				
#				
#				try:
#					if not os.path.exists(Clog):
#						os.mkdir(Clog)
#					if not os.path.exists(Cerr):
#						os.mkdir(Cerr)
#					if not os.path.exists(Olog):
#						os.mkdir(Olog)
#				except Exception, err:
#					raise Exception("Error: %s "%str(err))
#				
#
#				#print f, sRun, Out_EHE, Clog, Cerr, Olog
#				SubmitArgs.append([GCDFile, f, Out_EHE, r, sRun, Clog, Cerr, Olog])
#				
#print len(SubmitArgs)
##print SubmitArgs[0]
##print SubmitArgs[1]
#
#cPickle.dump(SubmitArgs,open("SubmitArgs_EHE.dat","w"))
#
#				
#				
#				
#                