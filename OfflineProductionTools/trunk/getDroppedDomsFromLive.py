"""
Manage dropped dom information. Intended to provide tools to synchronize databases
"""

import json
import urllib
import urllib2
import sys
import pymysql as MySQLdb

import SQLClient_i3live as live
m_live = live.MySQL()

import SQLClient_dbs2 as dbs2
dbs2_ = dbs2.MySQL()

import SQLClient_dbs4 as dbs4
dbs4_ = dbs4.MySQL()

from time import sleep
from datetime import timedelta,datetime

#####################################################

class MonsI3OmDb:
    """
    Factory for Mons I3OmDb test
    """

    def __init__(self,db_name="I3OmDb_test"):
        self.db = MySQLdb.connect("icedb.umons.ac.be","www","",db_name ) 
        self.cursor = self.db.cursor()

    def execute(self,sql):
        return self.cursor.execute(sql)

    def fetchall(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def __del__(self):
        """
        close stale mysql connection
        """
        self.cursor.close()
        self.db.close()

#####################################################

class DroppedDom:
    """
    Provides a model for a dropped Dom entry in either livedb or 
    I3OmDb. Can be sorted and compared
    """

    def __init__(self,**kwargs):
        self._missing = False
        if set(kwargs.values()) == set(["NULL"]):
            self.is_nil = True
        else:
            self.is_nil = False

        for k in kwargs.keys():
            if k == "RunId" or k == "run_id" or k == "runNumber":
                self.RunId = kwargs[k]
            if k == "StringId" or k == "string":
                self.StringId = kwargs[k]
            if k == "TubeId" or k == "position":
                self.TubeId = kwargs[k]    
            if k == "DropTime" or k == "t":
                self.DropTime = kwargs[k]
            if k == "Source" or k == "source":
                self.Source = kwargs[k]

    def set_missing(self):
        self._missing = True

    @property
    def missing(self):
        return self._missing

    def __eq__(self,other):
        samerun        = self.RunId == other.RunId
        samestring     = self.StringId == other.StringId
        samedom        = self.TubeId == other.TubeId

        # don't require equality for droptime and source, 
        # rather check this if necessary
        #samedroptime   = self.DropTime == other.DropTime
        #samesource     = self.Source == other.Source
        #if reduce(lambda x,y : x + y,[samerun,samestring,samedom,samedroptime,samesource]) == 5:
        if reduce(lambda x,y : x + y,[samerun,samestring,samedom]) == 3:
            return True
        else:
            return False

    def __lt__(self,other):
        if self == other:
            if self.DropTime < other.DropTime:
                return True
            else:
                return False
        else:
            if self.RunId < other.RunId:
                return True
            if self.RunId > other.RunId:
                return False
            if self.RunId == other.RunId:

                if self.StringId < other.StringId:
                    return True
                elif self.StringId == other.StringId:
                    if self.TubeId < other.TubeId:
                        return True
                    else:
                        return False

    def __gt__(self,other):
        if self == other:
            if self.DropTime > other.DropTime:
                return True
            else:
                return False
        else:
            if self.RunId > other.RunId:
                return True
            if self.RunId < other.RunId:
                return False
            if self.RunId == other.RunId:
                if self.StringId > other.StringId:
                    return True
                elif self.StringId == other.StringId:
                    if self.TubeId > other.TubeId:
                        return True
                    else:
                        return False

    def __le__(self,other):
        return (self.__lt__(self,other) or self.__eq__(self,other))

    def __ge__(self,other):
        return (self.__gt__(self,other) or self.__eq__(self,other))

    def __eq__(self,other):
        samerun        = self.RunId == other.RunId
        samestring     = self.StringId == other.StringId
        samedom        = self.TubeId == other.TubeId
        if reduce(lambda x,y : x + y,[samerun,samestring,samedom]) == 3:
            return True
        else:
            return False

    def __repr__(self):
        if self.is_nil:
            return """<DroppedDom: NIL>"""
        else:
            return """<DroppedDom: RunId %i StringId %i TubeId %i DropTime %s Source %s>""" %(self.RunId, self.StringId, self.TubeId, str(self.DropTime), self.Source)

    def __hash__(self):
        """
        To allow the use of doms in a set, they need to be
        hashable. We don't care about droptimes at this moment, 
        the idea behind that is that doms should only drop
        once in a run
        """
        if self.is_nil:
            return 0
        else:
            x = int(str(self.RunId) + "0" + str(self.StringId) + "0" + str(self.TubeId))
            return x


    def same_source(self,other):
        if self == other:
            if self.Source == other.Source:
                return True
            else:
                return False
        else:
            return False

    def same_droptime(self,other):
        if self == other:
            if self.DropTime == other.DropTime:
                return True
            else:
                return False
        else:
            return False
        
        
#####################################################

def NilDomFactory():
    """
    Creates a NIL Dom object
    """
    dom = DroppedDom(RunId = "NULL", StringId = "NULL", TubeId = "NULL", DropTime = "NULL" , Source = "NULL")
    dom.is_nil = True 
    return dom

#####################################################

def pDAQDomFactory(**kwwargs):
    """
    Patch the source field of a DroppedDom to say "pDAQ"
    """
    
    if kwargs.has_key("Source"):
        kwargs["Source"] = "pDAQ"
    if kwargs.has_key("source"):
        kwargs["source"] = "PDAQ"
    return DroppedDom(**kwargs)

#####################################################

class DBS2_SQL:
   
    def __init__(self,tablename="BadDomsDropped",lock=False):
        self.tablename = tablename
        self.sql = """"""
        self.lock = lock
        if self.lock:
            self.sql = """LOCK TABLES `%s` WRITE;""" %self.tablename
        self.first_insert = True
        self.need_trailing_semicolon = False
        self.finished = False

    def add_update(self,dom):
        self.sql += """UPDATE `%s` SET DropTime="%s" where RunId=%s and StringId=%s and TubeId=%s;
""" %(self.tablename,dom.DropTime,dom.RunId, dom.StringId, dom.TubeId)
    def add_insert(self,dom):
        if self.first_insert:
            self.sql +=  """INSERT INTO `%s` VALUES  """ %self.tablename
            self.first_insert = False
        self.sql += """(%i, %i, %i, "%s", "%s"),""" %(dom.RunId, dom.StringId, dom.TubeId,"pDAQ", dom.DropTime)
        self.need_trailing_semicolon = True

    def finish(self):
        if self.need_trailing_semicolon:
            self.sql = self.sql[:-1]
            self.sql += """;
"""
            self.need_trailing_semicolon = False
        if not self.finished:
            if self.lock:    
                self.sql += """UNLOCK TABLES;"""
            self.finished = True
    
    def __repr__(self):
        return self.sql
   
    def pretty_print(self):
        print self.sql
    
#####################################################
# DB Fetchers
#####################################################

_live_run_fetcher = lambda first_run_id : m_live.fetchall("""select r.runNumber,sr.good_i3,sr.good_it,sr.good_tstart,sr.good_tstop,r.tStart,r.tStop from livedata_snapshotrun as sr join livedata_run as r on sr.run_id = r.id where r.runNumber>=%s order by r.runNumber asc;""" %(first_run_id),UseDict=True)
_dbs4_run_fetcher = lambda first_run_id : dbs4_.fetchall("""select run_id ,good_i3, good_it, good_tstart, good_tstop from grl_snapshot_info where run_id>=%s order by run_id asc;""" %(first_run_id),UseDict=True)
_live_dropped_fetcher = lambda first_run_id : m_live.fetchall("""select dd.t,dd.source,d.string,d.position,r.runNumber from livedata_droppeddom as dd join livedata_dom as d on dd.d_id = d.id join livedata_run as r on dd.run_id = r.id where r.runNumber>=%s order by dd.t asc, d.position asc;""" %(first_run_id),UseDict=True)
_dbs2_dropped_fetcher = lambda first_run_id : dbs2_.fetchall("""select * from BadDomsDropped where RunId>=%s order by DropTime asc, TubeID asc""" %(first_run_id),UseDict=True)



################################################

def lastUpdatedMonsTestDb():
    """
    Last update of I3OmDb_test @Mons
    """
    mons = MonsI3OmDb() 
    last_updated = mons.fetchall("""select update_time from information_schema.tables where table_schema="I3OmDb" and table_name="BadDomsDropped"; """)
    last_run = mons.fetchall("""select RunId from BadDomsDropped order by RunId desc limit 1;""")
    print "I3OmDb_test@Mons was last updated on %s, the latest run is %i" %(last_updated[0][0],last_run[0][0])
    return last_updated[0][0],last_run[0][0]

################################################

def lastUpdatedDBS2():
    """
    Print when the database was updated the last time
    """
    last_updated = dbs2_.fetchall("""select update_time from information_schema.tables where table_schema="I3OmDb" and table_name="BadDomsDropped"; """)
    last_run = dbs2_.fetchall("""select RunId from BadDomsDropped order by RunId desc limit 1;""")
    print "I3OmDb on dbs2 was last updated on %s, the latest run is %i" %(last_updated[0][0],last_run[0][0])
    return last_updated[0][0],last_run[0][0]

################################################

def SwitchRuns(runids):
    """
    Return switch runs in the specific run range

    Args:
        runids (list): runs to query

    Returns (list)
    """
    if not runids:
        return runids
    runids = sorted(runids)
    switchruns = m_live.fetchall("""select switched_start,RunNumber from livedata_run where RunNUmber between %i and %i order by RunNumber asc""" %(min(runids),max(runids)))
    switchruns = filter(lambda x : x[0],switchruns)
    switchruns = [x[1] for x in switchruns]
    return switchruns
    
################################################

def _alignmentCreator(droppedA,droppedB,nameA="",nameB=""):
    """
    Extends the shorter of the two listswith Null entries
    to match the longer list
    Applies to I3OmDb style dropped dom information
    """
    
    all_runsA = set([x.RunId for x in droppedA])
    all_runsB = set([x.RunId for x in droppedB])

    if all_runsA != all_runsB:
        print "Runs are missing!"
        if all_runsA <  all_runsB:
            print "Missing the following runs in %s" %nameA
            print all_runsB - all_runsA

        if all_runsA >  all_runsB:
            print "Missing the following runs in %s" %nameB 
            print all_runsA - all_runsB
        sys.exit(1)
    
    # rename and clean up
    runs = all_runsA
    
    del all_runsA
    del all_runsB

    # figure out how many dropped doms are there per run
    def _identify_dropped_per_run(dropped):
        num_dropped = dict()
        total_dropped  = 0
        for r in runs:
            run_dropped = 0
            for d in dropped:
                if d.RunId == r:
                    run_dropped += 1
            total_dropped += run_dropped
            num_dropped[r] = run_dropped
        return num_dropped,total_dropped

    dropped_per_runA,total_droppedA = _identify_dropped_per_run(droppedA)
    dropped_per_runB,total_droppedB = _identify_dropped_per_run(droppedB)

    print "---- Found %i dropped doms in A" %total_droppedA
    print "---- Found %i dropped doms in B" %total_droppedB
    #print dropped_per_runA
    #print dropped_per_runB
    # identify the difference
    A_dropped_surplus  = []
    B_dropped_surplus  = []
    for r in runs:
        if dropped_per_runA[r] < dropped_per_runB[r]:     
            B_dropped_surplus.append(r)
        if dropped_per_runA[r] > dropped_per_runB[r]:     
            A_dropped_surplus.append(r)

    if A_dropped_surplus:
        print "-- %i runs had in total %i more dropped doms in %s (might be bad runs though)" %(len(A_dropped_surplus),(total_droppedA - total_droppedB),nameA)
        #print "-- %s" %Adropped_surplus.__repr__()
        surplus = total_droppedA - total_droppedB
        slave_list = droppedB
        master_list = droppedA
    if B_dropped_surplus:
        print "-- %i runs had in total %i more dropped doms in %s (might be bad runs though)" %(len(B_dropped_surplus),(total_droppedB - total_droppedA),nameB)
        #print "-- %s" %B_dropped_surplus.__repr__()
        surplus = total_droppedB - total_droppedA
        slave_list = droppedA
        master_list = droppedB
    if A_dropped_surplus and B_dropped_surplus:
        print "Something's weird.. check manually database integrity"
        sys.exit(1)
   
    master_list = sorted(master_list)
    slave_list  = sorted(slave_list) 
    aligned = [] # the new slave list with the lenght of the 
                 # master list
    i,j = 0,0 # a simple index
    same = 0
    nil  = 0
    difftime = 0
    start = 0
    while i < len(master_list):
     
        master = master_list[i]
        try:
            slave  = slave_list[j]
        except IndexError:
            # just use the last slave, it is fine
            pass
        if master == slave:
            if master.same_droptime(slave):
                aligned.append(master)
                i += 1
                j += 1
                same += 1
            else:
                # different droptime
                aligned.append(slave)
                i += 1
                j += 1
                difftime += 1
        else:
           # this dom is not present in the slave list
           # enter NullDom
           aligned.append(NilDomFactory())
           i += 1 #don't increment J
           nil +=1
    
    #print nil, surplus
    #print len(aligned),len(master_list), len(slave_list)
    #print nil, same,difftime
    assert len(aligned) == len(master_list)
    return aligned            

##########################################

def getDroppedDomsForRunJSON(run_id):
    """
    get the dropped doms for a specific run from i3live
    """

    # Define some variables
    u = 'icecube'
    p = 'skua'
    host = 'live.icecube.wisc.edu'
    url = 'https://%s/dropped_dom_json/%s/' % (host, run_id)
    
    # Fetch the JSON data
    req = urllib2.Request(url, urllib.urlencode({'user':u, 'pass':p}))
    response = urllib2.urlopen(req).read()
    
    # Parse JSON -> python dict
    dropdict = json.loads(response)
    
    # Use it at your convenience
    #for dd in dropdict['user_alert']:
    #    print dd
    
    # relax a bit to not stress out the i3live webservice so much
    #sleep(2)
    return dropdict

#############################################

def TranslateJson(jsondict,runid):
    """
    Transform the dict json style from 
    the webservice to the style from
    mysql

    Args:
        jsondict (dict): json from live
        runid (int): which run is this?
    """
    key_map = {'dom_string' : 'string',\
               'dom_position' : 'position'}
    def _translate(thisdict,source,runid):
        new = {}
        for k in thisdict.keys():
            if k in key_map:
                new[key_map[k]] = thisdict[k]
            if k == "drop_time":
                new["t"] = datetime.strptime(thisdict[k],"%Y-%m-%d %H:%M:%S") #e.g. 2016-02-08 19:41:07
        new['source'] = source
        new['runNumber'] = runid
        return new

    newAdoms = [_translate(this,'A',runid) for this in jsondict['user_alert']]
    newMdoms = [_translate(this,'M',runid) for this in jsondict['moni_file']]
    return newAdoms + newMdoms

#u'runNumber': 127535, u'position': 57, u't': datetime.datetime(2016, 2, 8, 19, 41, 7), u'string': 54, u'source': 'A'}
    
#{u'user_alert': [{u'dom_string': 54, u'drop_time': u'2016-02-08 19:41:07', u'dom_position': 57, u'dom_name': u'Spinachish', u'dom_mbid': u'83064ba9e408'}], u'moni_file': [{u'dom_string': 54, u'drop_time': u'2016-02-08 19:42:25', u'dom_position': 57, u'dom_name': u'Spinachish', u'dom_mbid': u'83064ba9e408'}]}





#############################################


def getDroppedDoms(first_run_id,fetcher=_live_dropped_fetcher,source="nodoubles",last_run_id=None):
    """
    Query databse for dropped doms

    Args:
        first_run_id (int): Database is queried with a 'where >= first_run_id' statement
    Keyword Args:
        fetcher (func): A predefined function evaluating to a query
        source (str): all|nodoubles|M|A|pDAQ|monitoring|other
        last_run_id (int): get runs up to this run

    Returns (list): List of DroppedDom objects
                      
    """
    dropped = fetcher(first_run_id)
    
    # figure out which key to use
    if "runNumber" in dropped[0]:
        runkey = "runNumber"
    else:
        runkey = "RunId"
    # we don't care about bad runs
    runs = getRunsWithStatus(first_run_id,summarize=False,onlygood=True)
    runs = [r["runNumber"] for r in runs]
    dropped = filter(lambda x : x[runkey] in runs, dropped)
    if last_run_id is not None:
        dropped = [dom for dom in dropped if dom[runkey] <= last_run_id]
    if runkey == "runNumber": # makes only sense for live
        runs = getRunsWithStatus(first_run_id,summarize=False)
        runs = [x["runNumber"] for x in runs] 
        if last_run_id is not None:
            runs = filter(lambda x : x <= last_run_id,runs)
        runs = set(runs)
        for run in SwitchRuns(runs):
            print "-- Found Switch run %i "%run
            switchdoms = getDroppedDomsForRunJSON(run)  
            switchdoms = TranslateJson(switchdoms,run)
            dropped.extend(switchdoms)
    if source == "all":
        return sorted(list([DroppedDom(**dom) for dom in dropped]))
    elif source == "nodoubles": 
        # take care of double entries by using set
        # FIXME: prefer 'A' source
        am_doms = sorted(list([DroppedDom(**dom) for dom in dropped]))
        a_doms = filter(lambda x : x.Source == 'A',am_doms)
        m_doms = filter(lambda x : x.Source == 'M',am_doms)
        return sorted(set(a_doms+m_doms))
        #return  sorted(list(set([DroppedDom(**dom) for dom in dropped])))
    else:
        #two steps as the key 'source' might be different
        data =  sorted([DroppedDom(**dom) for dom in dropped])
        return filter(lambda x : x.Source == source, data)

################################################

def getRunsWithStatus(first_run_id,fetcher=_live_run_fetcher,summarize=True,onlygood=False):
    """
    Returns a status (good,bad) for all runs >= first_run_id

    Args:
        fetcher (func): provide db and sql

    Keyword Args:
        summarize (bool): group status, start and stop in a dictionary
        onlygood (bool): get only good runs
    """
    runs = fetcher(first_run_id)
    if not summarize:
        if onlygood:
            runs = filter(lambda x :x["good_i3"] + x["good_it"] > 0,runs) 
        return runs 
    else:
        output,status,start,stop = dict(),dict(),dict(),dict()
        for r in runs:
            if onlygood:
                if (r["good_i3"] + r["good_it"]) == 0:
                    continue
            if not r.has_key("runNumber"):
                r["runNumber"] = r["run_id"]
            status[r["runNumber"]] = r["good_i3"] + r["good_it"]
            start[r["runNumber"]] = r["good_tstart"]
            if start[r["runNumber"]] == None:
                if r.has_key("tStart"):
                    start[r["runNumber"]] = r["tStart"]

            stop[r["runNumber"]] = r["good_tstop"]
            if stop[r["runNumber"]] == None:
                if r.has_key("tStop"):
                    stop[r["runNumber"]] = r["tStop"]
        output["status"] = status
        output["start"] = start
        output["stop"] = stop
        return output

################################################

def checkIfDomInI3OmDb(dropped):
    """
    See if a Dom is already in the I3OmDb database

    Args:
        dropped (DroppedDom): check if this dom is present in I3OmDb
    """

    sleep(.1)
    droppeddb = dbs2_.fetchall("""select * from BadDomsDropped where RunId=%s and StringId=%s and TubeID=%s""" %(dropped.RunId, dropped.StringId, dropped.TubeId),UseDict=True)
    if len(droppeddb):
        return True
    else:
        return False

################################################

def UpdateI3OmDb(usemons=False,source="nodoubles",add_lock=False):
    """
    Provide sql snipped to be inserted in I3OmDb

    Keyword Args:
        usemons (bool): Use the I3OmDb_test@Mons instead Madison DBS2
        source (str): Livedb will be queried for this source field, magic keywords
        add_lock (bool): Add SQL lines which lock/unlock the table for writing
    """
    if usemons:
        __, last_run = lastUpdatedMonsTestDb()
    else:
        __, last_run = lastUpdatedDBS2()
    run_status = getRunsWithStatus(last_run)
    good_runs   = [ run for run in run_status["status"].keys() if run_status["status"][run]] 
    live_dropped = getDroppedDoms(last_run,source=source)   
    print "-- got %i dropped Doms from live for %i good runs" %(len(live_dropped),len(good_runs)) 
    sql = DBS2_SQL(lock=add_lock)
    for dom in live_dropped:
        if dom.RunId in good_runs:
            if dom.DropTime < run_status["start"][dom.RunId]:
                # fix it!
                print "-- -- adjusting DropTime for %s to " %dom.__repr__(), run_status["start"][dom.RunId]
                dom.DropTime = run_status["start"][dom.RunId] + timedelta(0,1)
                sql.add_insert(dom)
            elif dom.DropTime > run_status["stop"][dom.RunId]:
                # we don't care
                continue
            else:
                sql.add_insert(dom)

    sql.finish()
    print "-- -- printing sql.."
    sql.pretty_print()
    return sql

################################################

def getLatestTriggerConfigId():
    """
    Check the databases for the latest Triggerconfig

    Returns (tuple):
        TriggerConfiguration Ids from mons as well as dbs2
    """
    mons_db = MonsI3OmDb(db_name="I3OmDb")

    latest_triggerid_mons = mons_db.fetchall("select ConfigurationId from TriggerConfigurationList order by ConfigurationId desc limit 1;")
    latest_triggerid_dbs2 = dbs2_.fetchall("select ConfigurationId from TriggerConfigurationList order by ConfigurationId desc limit 1;")
    latest_triggerid_dbs2 = latest_triggerid_dbs2[0][0]
    latest_triggerid_mons = latest_triggerid_mons[0][0]
    print latest_triggerid_dbs2, latest_triggerid_mons

    print "-- latest configureation id dbs2: %i -- latest configuration id mons: %i" %(latest_triggerid_dbs2,latest_triggerid_mons)
    return latest_triggerid_dbs2,latest_triggerid_mons

################################################

def UpdateTriggerConfiguration():
    """
    Check Mons database and see if a new trigger config has been inserted
    If, then update dbs2 accordingly
    """

    tid_dbs2,tid_mons = getLatestTriggerConfigId()
    mons = MonsI3OmDb(db_name="I3OmDb") 
    if tid_mons > tid_dbs2:
        mons_TriggerConfigurationName = mons.fetchall("""SELECT ConfigurationId, ConfigurationName from TriggerConfigurationName where ConfigurationId > %i""" %tid_dbs2)
        mons_TriggerConfigurationList = mons.fetchall("""SELECT ConfigurationId,TriggerId from TriggerConfigurationList where ConfigurationId > %i""" %tid_dbs2)
        mons_TriggerIds = mons.fetchall("""select TriggerId from TriggerConfigurationList where ConfigurationId > %i""" %tid_dbs2)
        # trigger id is primary key, so we should be fine by 
        # by taking only the first of it and say get
        first_mons_tid = min(list(mons_TriggerIds))
        mons_TriggerConfigurationListDetail = mons.fetchall("""SELECT TriggerId, TriggerType, TriggerConfigId, SourceId from TriggerConfigurationListDetail where TriggerId >= %i""" %first_mons_tid) 
        #sql = """LOCK tables `TriggerConfigurationName` WRITE;
#"""
        sql = ""
        for i in mons_TriggerConfigurationName:
            sql += """INSERT INTO TriggerConfigurationName (ConfigurationId, ConfigurationName) VALUES (%i,%s);
""" %i
        #sql += """LOCK tables `TriggerConfigurationList` WRITE;
#"""
        for i in mons_TriggerConfigurationList:
            sql += """INSERT INTO TriggerConfigurationList (ConfigurationId,TriggerId) VALUES (%i,%i);
""" %i
        #sql += """LOCK tables `TriggerConfigurationListDetail` WRITE;
#"""
        for i in mons_TriggerConfigurationListDetail:
            sql += """INSERT INTO TriggerConfigurationListDetail (TriggerId, TriggerType, TriggerConfigId, SourceId) VALUES (%i,%i,%i,%i);
""" %i
        #sql += """UNLOCK tables;""" 
        print sql

################################################

def insertDroppedDoms(sql,mons=True):
    """
    Insert dropped doms in the database at mons or dbs2
    
    Args:
        sql (str): valid SQL code with insert statements
    
    Keyword Args:
        mons (bool): use the database at Mons
    """
    if mons:
        db = MonsI3OmDb(db_name="I3OmDb_test")
    else:
        db = dbs4
   
    print "-- inserting sql %s in %s --" %(sql,("I3OmDb_test@Mons" if mons else "DBS2"))
    db.execute(sql.sql)
    return 

################################################

def FillMissingRunsDBS2(first_run_id,last_run_id=None):
    """
    If a run in dbs2 is missing, get the dropped doms for that
    run from live
    
    Args:
        first_run_id (int): start from this run

    Keyword Args:
        last_run_id (int): only go up to this run

    """
    return

################################################

def checkBadDomsDroppedvsLive(first_run_id,verbose=False,print_sql=True,source="A",last_run=None):
    """
    check if the dropped dom information which can be found in  the baddomsdropped table
    is consistent with the live databese

    Args:
        first_run_id (int): Start querying with this run
    
    Keyword Args:
        verbose (bool): print basically every dom encountered
        print_sql (bool): print out sql statements which allow to update dbs2/I3OmDb
        source (str): specify if only entries from a specific source in i3live should be considered
        last_run (int): only check until this run
    """

    live_dropped   = getDroppedDoms(first_run_id,fetcher = _live_dropped_fetcher,source=source,last_run_id=last_run)

    I3OmDb_dropped = getDroppedDoms(first_run_id,fetcher = _dbs2_dropped_fetcher,source='all',last_run_id=last_run)
    if last_run is not None:
        live_dropped =  filter(lambda x : x.RunId <= last_run,live_dropped)
        I3OmDb_dropped = filter(lambda x : x.RunId <= last_run,I3OmDb_dropped)
    
    # crop live_dropped to last run which can be
    # be found in I3OmDb
    last_I3OmDb_run = sorted(map(lambda x : x.RunId,I3OmDb_dropped))[-1]
    live_dropped = filter(lambda x : x.RunId <= last_I3OmDb_run, live_dropped)
    omdbruns = set([x.RunId for x in I3OmDb_dropped])
    liveruns = set([x.RunId for x in live_dropped])
    if not omdbruns == liveruns:
        omdbmissingruns = liveruns - omdbruns
        print "WARNING: Entire Runs are missing in DBS2!"
        print "-- The following runs are missing:", omdbmissingruns
        for run in omdbmissingruns: # might have gaps
            print "-- -- Adding run %i to dbs2" %run
            thisrundoms = getDroppedDoms(run,fetcher= _live_dropped_fetcher,last_run_id=run)
            newdoms = []
            for dom in thisrundoms:
                dom.set_missing()
                newdoms.append(dom)
            I3OmDb_dropped.extend(newdoms)
    #print len(live_dropped) == len(set(live_dropped))
    print "Found %i table lines in live and %i table lines in I3OmDb" %(len(live_dropped),len(I3OmDb_dropped))
    if len(live_dropped) > len(I3OmDb_dropped):
        print "Warning, more table lines in livedata_droppeddom!"
        print "-- livedata_droppeddom has %i more entries than BadDomsDropped" %(len(live_dropped) - len(I3OmDb_dropped)) 
        print "-- creating alingment with adding 'NULL' entries...."
        I3OmDb_dropped = _alignmentCreator(I3OmDb_dropped,live_dropped,nameA="BadDomsDropped",nameB="live")
        #live_dropped = map(lambda x : DroppedDom(**x), live_dropped)

    if len(live_dropped) < len(I3OmDb_dropped):
        print "Warning, more table lines in BadDomsDropped!"
        print "-- BadDomsDropped has %i more entries than livedata_droppeddom" %(len(I3OmDb_dropped) - len(live_dropped))
        print "-- creating alingment with adding 'NULL' entries...."
        live_dropped = _alignmentCreator(live_dropped,I3OmDb_dropped,nameA="live",nameB="BadDomsDropped")
        #I3OmDb_dropped = map(lambda x : DroppedDom(**x), I3OmDb_dropped)
    
    assert len(live_dropped) == len(I3OmDb_dropped),"Error, list did not get aligned! len(live) %i len(dbs) %i " %(len(live_dropped),len(I3OmDb_dropped))

    live_dropped = sorted(live_dropped)
    I3OmDb_dropped = sorted(I3OmDb_dropped)

    # compare the entries one by one

    fine_runs    = []
    problem_runs = []
    problems = 0

    run_status = getRunsWithStatus(first_run_id,fetcher=_dbs4_run_fetcher)
    good_runs   = [ run for run in run_status["status"].keys() if run_status["status"][run]] 
    
    too_early = []
    too_late = []
    missing_entries = []
    update_entries  = []
    for i in xrange(len(live_dropped)):
        livedom = live_dropped[i]
        omdbdom = I3OmDb_dropped[i]
        print livedom
        print omdbdom
        print "-----"
        #if livedom.RunId == 127190:
        #    print livedom,"live"
        #if omdbdom.RunId == 127190:
        #    print omdbdom,"omdb"
        if livedom != omdbdom:         
            if livedom.RunId in good_runs:
                if verbose: print "live: ", livedom
                if verbose: print "OmDb: ", omdbdom
                if omdbdom.is_nil:
                    missing_entries.append(livedom)
                    problem_runs.append(livedom.RunId)
                    problems += 1
                else:
                    raise ValueError("TableAlignment corrupt!i")
        elif not livedom.same_droptime(omdbdom):
            #if livedom.is_same_dom(omdbdom):
            # DropTime is different!
            # check if it is too early
            if livedom.DropTime < run_status["start"][livedom.RunId]:
                # fix it!
                livedom.DropTime = run_status["start"][livedom.RunId] + timedelta(0,1)
                too_early.append(livedom)
                if omdbdom.missing:
                    #somebody claimed this one missing,
                    #better add it...
                    missing_entries.append(livedom)
                else:
                    update_entries.append(livedom)        
                problem_runs.append(livedom.RunId)
                problems += 1
            if livedom.DropTime > run_status["stop"][livedom.RunId]:
                too_late.append(livedom)
        elif omdbdom.DropTime < run_status["start"][omdbdom.RunId]:
            # previously overlooked problem...
            omdbdom.DropTime = run_status["start"][omdbdom.RunId] + timedelta(0,1)
            too_early.append(omdbdom)
            if omdbdom.missing:
                missing_entries.append(omdbdom)
            else:
                update_entries.append(omdbdom)        
            problem_runs.append(omdbdom.RunId)
            problems += 1

        else:
            pass
            # all fine:)
    problem_runs = sorted(set(problem_runs))
    #print "Missing entries",len(missing_entries)
    #print "Update entries",len(update_entries)
    #print "Too early" , len(too_early)
    #print "Too late", len(too_late)
    update_entries += filter(checkIfDomInI3OmDb,missing_entries)
    missing_entries = filter(lambda x : not checkIfDomInI3OmDb(x),missing_entries)
    #print len(missing_entries)
    print "-- %i good runs found where dropped dom info in dbs2 and live mismatch: " %len(problem_runs) + ",".join(map(str, problem_runs))
    print "-- In total %i dropped doms in all good runs where tables in dbs2 and live mismatch" %problems
    #print "%i problems found in %i runs" %(problems,len(problem_runs))
    print "-- -- might need to UPDATE %i table rows..." %len(update_entries)
    print "-- -- might need to INSERT %i new table rows..." %len(missing_entries)
    print "-- %i doms dropped earlier than run start!" %len(too_early)
    #print "-- --", too_early
    print "-- %i doms dropped after the run end!" %len(too_late)

    # let's update BadDomsDropped Table
    # policy: 1) We don't care about doms dropping AFTER the run ends
    #         2) Doms which dropped before the run started
    #            get a drop time run start + 1 sec
    #         3) We don't care about bad runs
    #         4) For good runs, we trust live
    
    if print_sql:
        print """ --- will provide sql for %i doms""" %(len(update_entries) + len(missing_entries))
        print "+-"*20
        #print """LOCK TABLES `BadDomsDropped` WRITE;"""
        sql = DBS2_SQL()
        
        for dom in update_entries:
            sql.add_update(dom)
        for dom in missing_entries:
            sql.add_insert(dom)
        sql.finish()
        sql.pretty_print()

if __name__ == "__main__":

    from optparse import OptionParser

    parser = OptionParser(usage="""usage: %prog [OPTIONS]  """,
    description="Check information about dropped doms in the livedata_droppeddom database and I3OmDb. Use to synchronize databases.",
    )
    parser.add_option("--update-i3omdb", dest="updatei3omdb", action="store_true", default=False, help="Print needed SQL to synchronize I3OmDb with live db.")
    parser.add_option("--mons", dest="mons", action="store_true", default=False, help="Use I3OmDb_test at Mons instead of DBS2")
    parser.add_option("--to-file",dest="tofile", action="store_true", default=False, help="Write SQL to file")
    parser.add_option("-s", dest="start_run", default=None, help="The first run the database will be queried for.")
    parser.add_option("--update-triggerconfig", dest="updatetc", action="store_true", default=False, help="get the latest triggerconfig from mons and provide sql for update.")
    parser.add_option("-v", dest="verbose", action="store_true", default=False, help="increase verbosity.")
    parser.add_option("--insert-in-db", dest="insertindb", action="store_true", default=False, help="insert the information already in the database.")
    parser.add_option("--source", dest="source", default="nodoubles", help="The source in live where the information comes for. It might be 'nodoubles', then double entries are filtered out. It might be 'A', then when entries are doubled, the user alert data is returned. In case of 'M', the moni information is returned. Finally 'all' returns all entries (including doublets)")
    opts,args = parser.parse_args()
    if opts.updatetc:
        UpdateTriggerConfiguration()
        sys.exit(0)

    if opts.updatei3omdb:
        sql = UpdateI3OmDb(usemons=opts.mons,source=opts.source)
        if opts.tofile:
            f = open("db_updates.sql","w")
            f.write(sql.sql)
            f.close()
        if opts.insertindb:
            if sql.sql:
                insertDroppedDoms(sql,mons=opts.mons)
            else:
                print "Nothing to insert... %s" %sql
        sys.exit(0)
    else:
    # first run was 127024
        checkBadDomsDroppedvsLive(opts.start_run,verbose=opts.verbose,source=opts.source)



