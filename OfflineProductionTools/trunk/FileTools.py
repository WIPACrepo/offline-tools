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
from dummylogger import DummyLogger

class FileTools(object):
    def __init__(self,FileName,logger=DummyLogger()):
        self.FileName = str(FileName)
        self.logger = logger

        if not os.path.isfile(FileName):
            self.logger.warning("%s does not exist"%FileName)

    def md5sum(self,buffersize=16384):
        """Return md5 digest of file"""

        self.logger.debug("Try to open file for md5 sum: %s"%self.FileName)

        with open(self.FileName) as filed:
            try:
                import hashlib
            except ImportError:
                import md5
                digest = md5.new()
            else:
                digest = hashlib.md5()
    
            self.logger.debug("Read file")

            buffer = filed.read(buffersize)
            while buffer:
                digest.update(buffer)
                buffer = filed.read(buffersize)

            self.logger.debug("Close file")
            filed.close()
            return digest.hexdigest()
