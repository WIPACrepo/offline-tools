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
            self.logger.warning("%s does not exist" % FileName)

    def md5sum(self, buffersize = 16384):
        return self.checksum('md5', buffersize)

    def sha512(self, buffersize = 16384):
        return self.checksum('sha512', buffersize)

    def checksum(self, type, buffersize = 16384):
        """Return checksum of type `type` digest of file"""

        self.logger.debug("Try to open file for %s checksum sum: %s" % (type, self.FileName))

        with open(self.FileName) as filed:
            import hashlib
            digest = None

            if type.lower() == 'md5':
                digest = hashlib.md5()
            elif type.lower() == 'sha512':
                digest = hashlib.sha512()
    
            self.logger.debug("Read file")

            buffer = filed.read(buffersize)
            while buffer:
                digest.update(buffer)
                buffer = filed.read(buffersize)

            self.logger.debug("Close file")
            filed.close()
            return digest.hexdigest()
