#!/usr/bin/env python

import os, sys
import glob
from optparse import OptionParser
import datetime
import string
import re
from dateutil.parser import parse
from dateutil.relativedelta import *
from math import *

class FileTools(object):
    def __init__(self,FileName):
        self.FileName = str(FileName)
        if not os.path.isfile(FileName):
            print "%s does not exist"%FileName

    def md5sum(self,buffersize=16384):
        """Return md5 digest of file"""
        filed = open(self.FileName)
        try:       import hashlib
        except ImportError:
            import md5
            digest = md5.new()
        else:
            digest = hashlib.md5()
    
        buffer = filed.read(buffersize)
        while buffer:
            digest.update(buffer)
            buffer = filed.read(buffersize)
        filed.close()
        return digest.hexdigest()