#!/usr/bin/env python

import os,sys
import glob
import datetime

sys.path.append("/data/user/i3filter/SQLServers_n_Clients/")
try:
    import SQLClient_Simdbs4 as dbs4
    dbs4_ = dbs4.MySQL()
    
except Exception, err:
    raise Exception("Error: %s "%str(err))

SysChkDatasets = [10551,10552,10553,10554,10555,10556,10566]
#SysChkDatasets = [10551]

CheckTime = datetime.datetime.now()

print "\nAttempting to copy logs @: ",CheckTime

for S in SysChkDatasets:

    Site = dbs4_.fetchall("""SELECT reqs FROM task_def t
                             where dataset_id=%s and name="MainTask" """%S,UseDict=True)
    
    Site = Site[0]['reqs'].split("'")[1]
    siteDir = """/data/sim/scratch/IceSim/SysChkLogs/%s/%s/"""%(Site,S)
    
    if os.path.isdir(siteDir):
        files = glob.glob(siteDir+"/*")
        outDir = os.path.join(siteDir,"Logs_%s"%CheckTime.date())
        os.mkdir(outDir)
        print outDir

        jobsStatus = dbs4_.fetchall("""SELECT queue_id,status FROM job j where dataset_id=%s """%S,UseDict=True)
        jS = {}
        for j in jobsStatus:
            jS[str(j['queue_id'])] = j['status']


        for f in files :
            if "x509" in f or os.path.isdir(f): continue    # remove certificate file
            #if os.path.isdir(f) or "x509" not in f: continue
            td = CheckTime - datetime.datetime.fromtimestamp(os.path.getmtime(f))
            duration = td.seconds + (td.days * 24 * 3600)
            # only copy files created within the current SysChk window i.e. last 48hrs
            if duration < 172800:
                try:
                    f_status = os.path.join(outDir,os.path.basename(f)+"_"+jS[f.split('_')[-1]])
                    #print "cp %s %s"%(f,f_status)
                    os.system("cp %s %s"%(f,f_status))
                except:
                    pass
