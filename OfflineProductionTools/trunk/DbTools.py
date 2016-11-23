#!/usr/bin/env python

import os, sys
#import glob
#from optparse import OptionParser
#import datetime
#import string
#import re
#from dateutil.parser import parse
#from dateutil.relativedelta import *
#from math import *
##from lxml import etree as et
#import libxml2

class DbTools(object):
    def __init__(self,RunNumber,DatasetId):
        self.RunNumber = RunNumber
        self.DatasetId = DatasetId

    def AllOk(self):
        try:
            import SQLClient_dbs4 as dbs4
            dbs4_ = dbs4.MySQL()
        except Exception, err:
            raise Exception("Error: %s "%str(err))
        
        try:
            result = dbs4_.fetchall("""SELECT SUM(if (1,1,0)) - SUM(if (j.status="OK",1,0)) - SUM(IF(j.status = 'BadRun', 1, 0)) 
                           FROM i3filter.job j JOIN i3filter.run r ON j.queue_id=r.queue_id
                           WHERE j.dataset_id=%s AND r.dataset_id=%s AND r.run_id=%s"""%\
                          (self.DatasetId,self.DatasetId,self.RunNumber))

            if result[0][0] is None:
                print 'No records for %s, probably not submitted yet'%self.RunNumber
                return 1

            return int(result[0][0])
        except Exception, err:
            raise Exception("Error: %s "%str(err))
