"""
Check logs and check how severe the issues are
"""

import json
import urllib
import urllib2
import sys
import re

import getDroppedDomsFromLive as l

import SQLClient_i3live as live
m_live = live.MySQL()

import SQLClient_dbs2 as dbs2
dbs2_ = dbs2.MySQL()

import SQLClient_dbs4 as dbs4
dbs4_ = dbs4.MySQL()

from time import sleep
from datetime import timedelta,datetime

#########################################
# some patterns

#pfilt_file = "^PFFilt_PhysicsFiltering_Run00(?<runid>[0-9]{6}_Subrun000000_00000000.meta.xml
pfilt_file    = re.compile("^PFFilt_PhysicsFiltering_Run00(?P<runid>[0-9]{6})")
entering_db   = re.compile("^entering new records for run = (?P<runid>[0-9]{6})")
pfilt_empty   = re.compile("^Input file list is empty, maybe failed/short run")
pfilt_missing = re.compile("^Looks like we don't have all the files for this run")


def CheckGetRunInfo(logfilename,verbose=False):
    """
    Open the given logfile produced by GetRunInfo_2015.py
    and check for issues
    """
    successfully_entered_in_db = []
    pfilt_files = []
    no_files = []
    missing_files = dict()
    first = True 
    skipline = False   
    enter_db_record = False
    with open(logfilename,"r") as f:
        for line in f.readlines():
            if verbose: print line
            pfilt = pfilt_file.match(line)
            db    = entering_db.match(line)
            empty = pfilt_empty.match(line)
            missing = pfilt_missing.match(line) 
            if not pfilt is None:
                last_run = int(pfilt.groupdict()["runid"])
                pfilt_files.append(last_run)
            if not db is None:
                enter_db_record = True
                last_run = int(db.groupdict()["runid"])
            if (empty is None) and enter_db_record:
                successfully_entered_in_db.append(last_run)
                enter_db_record = False
            if not empty is None:
                # check if run is in goodruns
                if runs["status"][last_run]:
                    no_files.append(last_run)
            if missing:
                skipline = True
                missing_files[last_run] = "X"
            
            if first:
                print "Getting run information from dbs4 starting with run %i" %last_run 
                runs = l.getRunsWithStatus(last_run,fetcher=l._dbs4_run_fetcher,summarize=True)
                first = False

    print "No files were found on the filesystem for %i good runs %s" %(len(no_files),no_files.__repr__())
    print "%i runs were successfully entered in dbs4" %len(successfully_entered_in_db)
    #print pfilt_files


 
if __name__ == "__main__":

    from optparse import OptionParser

    parser = OptionParser(usage="""usage: %prog [-f logfile]  """,
    description="Greps through a logfile and tells you what to do.",
    )
    parser.add_option("-f", dest="file", default=None, help="The logfile to grep through.")
    #parser.add_option("--update-omdb", dest="update_omdb", default=False, action="store_true", help="Provide sql to update omdb with dropped dom information from live and exit.")
    #parser.add_option("-s", dest="start_run", default=None, help="The first run the database will be queried for.")
    #parser.add_option("-v", dest="verbose", action="store_true", default=False, help="increase verbosity.")
    opts,args = parser.parse_args()
    CheckGetRunInfo(opts.file)    
    #lastUpdatedDBS2()
    #if opts.update_omdb:
    #    UpdateI3OmDb()
    #    sys.exit(0)
    #else:
    # first run was 127024
    #    checkBadDomsDroppedvsLive(opts.start_run,verbose=opts.verbose,print_sql = opts.sql)

