#!/usr/bin/env python

import os, sys
import StringIO
import logging
import glob
import imp
from math import isnan
from I3Tray import *
from icecube import icetray, dataclasses, dataio

#sys.path.append("/net/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.0_amd64_IC2012-L2_V13-01-00/lib/icecube")
#
#libs = glob.glob("/net/user/i3filter/IC86_OfflineProcessing/icerec/RHEL_6.0_amd64_IC2012-L2_V13-01-00/lib/icecube/*.so")
#libs.sort()
##libs = libs[0:3]
#for lib in libs:
#	libName = (os.path.basename(lib)).replace(".so","")
#	try:
#		imp.load_dynamic(libName,lib)
#		#print "loading ",libName
#	except:
#		pass

def MakeWiki(Type,Keys,Objs,splits):
	#KnownType = 0
	with open("test_2013.txt",'a') as f:
		if Type=="Q":
			#KnownType=1
			f.write("""
==Q Frames==
Q frames contain the raw data, extracted and calibrated pulses and the reconstructions that were run at pole.
{| class="wikitable sortable"
|-
! style="width: 20em;" | Frame object !! style="width: 20em;" | Data type !! style="width: 40em;" class="unsortable" | Description !! style="width: 15em;" | Created by
			""")

			for k in Keys:
				f.write("""
|-
|<code>%s</code>||<code>%s</code>|| ||
						"""%(k,Objs[k]))

			f.write("""
|}
					""")	


		if Type=="P":
			#KnownType=1
			f.write("""
==P Frames==
Cleaned and/or split pulses as well as all offline reconstruction results are stored in P frames.
			""")

			for s in splits:
				f.write("""
===Sub-Event Stream: <code>%s</code>===
{| class="wikitable sortable"
|-
! style="width: 20em;" | Frame object !! style="width: 20em;" | Data type !! style="width: 40em;" class="unsortable" | Description !! style="width: 15em;" | Created by 
|-					
						"""%s)
				
				for k in Keys:
					if Objs[k][1] == s:
						f.write("""
|-
|<code>%s</code>||<code>%s</code>|| ||
							"""%(k,Objs[k][0]))
			
				f.write("""
|}
					""")



if __name__ == '__main__':
	
	if len(os.sys.argv) < 2:
		print "At least 1 file argument must be given"
		exit(1)

	i3Objs_p = {}
	i3Objs_q = {}
	splits = []
	splits_ = []
	
	inFiles = os.sys.argv[1:]
	#i3file = dataio.I3File(os.sys.argv[1])

	for inFile in inFiles:
		i3file = dataio.I3File(inFile)
	
		count = 0
		
		#while i3file.more() and count < 100 :
		while i3file.more():
				
			frame = i3file.pop_frame()

			
			for k in frame.keys():
				
				k_ = ""

				if len(k.split("Split")) > 1:
					try:
						int(k.split("Split")[1])
						k_ = k.split("Split")[0]+"SplitN"
					except:
						pass
					if not len(k_):
						try:
							int(k.split("Splits")[1])
							k_ = k.split("Splits")[0]+"SplitsN"
						except:
							pass
						
						
				if str(frame.Stop) == "DAQ" and k not in i3Objs_q and k_ not in i3Objs_q:
					try:
						#i3Objs[k] = frame[k].__class__.__name__
						
						#i3Objs_q[k] = frame.type_name(k)
						
						if len(k_):
							i3Objs_q[k_] = frame.type_name(k)
						else:
							i3Objs_q[k] = frame.type_name(k)
						
						
						#if frame['I3EventHeader'].sub_event_stream not in splits_ : splits_.append(frame['I3EventHeader'].sub_event_stream)
						
						#i3Objs_[k] = type(frame[k])
						#print k,frame.type_name(k)
					except:
						pass
						#print k
						
				if str(frame.Stop) == "Physics" and k not in i3Objs_q and k not in i3Objs_p and k_ not in i3Objs_q and k_ not in i3Objs_p:
					try:
						#i3Objs_p[k] = [frame.type_name(k),frame['I3EventHeader'].sub_event_stream]
						if len(k_):
							i3Objs_p[k_] = [frame.type_name(k),frame['I3EventHeader'].sub_event_stream]
						else:
							i3Objs_p[k] = [frame.type_name(k),frame['I3EventHeader'].sub_event_stream]
						

						if frame['I3EventHeader'].sub_event_stream not in splits : splits.append(frame['I3EventHeader'].sub_event_stream)
					except:
						pass
						
			count+=1

#
q_keys = i3Objs_q.keys()
q_keys.sort()
p_keys = i3Objs_p.keys()
p_keys.sort()
#


open("test_2013.txt",'w')
MakeWiki("Q",q_keys,i3Objs_q,splits)
MakeWiki("P",p_keys,i3Objs_p,splits)

##MakeWiki("Q")
#
##for p in p_keys:
##	print p,i3Objs_p[p]
##	#,i3Objs_[s]
###	
##print len(p_keys)
###


