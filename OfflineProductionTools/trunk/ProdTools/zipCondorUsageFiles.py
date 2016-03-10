#!/usr/bin/env python

import os,sys
import json
import subprocess as sub
import glob

workingDir = '/net/icecube-usr/i3filter'
jFiles = glob.glob(workingDir+'/*.json')
jFiles = [os.path.basename(j) for j in jFiles]
jFiles.sort()
OutTar = os.path.join(workingDir,'CondorUsage.tar')
sub.check_call(["tar","cf",OutTar,"-C",workingDir,jFiles[0]])
for j in jFiles[1:]:
    sub.check_call(["tar","rf",OutTar,"-C",workingDir,j])

