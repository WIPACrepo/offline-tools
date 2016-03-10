import sys, os
import datetime
import socket
import subprocess as sub

#import MySQLdb
#sys.path.append("/net/user/i3filter/SQLServers_n_Clients/npx4/")

try:
    import SQLClient_dbs4 as dbs4
    dbs4_ = dbs4.MySQL()
    
except Exception, err:
    raise Exception("Error: %s "%str(err))

if socket.gethostname() != "npx4.icecube.wisc.edu":
    print "This only works on the npx4 head node"
    exit(1)

print "Attempting clean up at ",str(datetime.datetime.now())


HeldJobs = sub.Popen(['condor_q', '-hold', 'i3filter'], stdout=sub.PIPE).communicate()[0]
HeldJobs = HeldJobs.split("\n")
HeldJobsId = [h.split(" ")[0] for h in HeldJobs if len(h) and len(h.split(" ")[0]) and str(h.split(" ")[0]).startswith('1')]
HeldJobsId = ",".join(HeldJobsId)
if len(HeldJobsId):

    UpdateToOK = dbs4_.fetchall("""select grid_queue_id from i3filter.job
                            where dataset_id=1871 and status="OK"
                            and grid_queue_id in (%s) """%(HeldJobsId),UseDict=True)
    
    if len(UpdateToOK):
        print "removing held jobs that were actually OK"
        for u in UpdateToOK:
            print "removing %s "%u['grid_queue_id']
            sub.call(['condor_rm','%s'%u['grid_queue_id']])
            
        
        
    Reset = dbs4_.fetchall("""select grid_queue_id from i3filter.job
                            where dataset_id=1871 and status!="OK"
                            and grid_queue_id in (%s) """%(HeldJobsId),UseDict=True)
    
    if len(Reset):
        r_ = ",".join([rs['grid_queue_id'] for rs in Reset])
        print "removing held jobs that were NOT OK"
        for r in Reset:
            print "removing %s "%r['grid_queue_id']
            sub.call(['condor_rm','%s'%r['grid_queue_id']])
        
        dbs4_.execute("""update i3filter.job
                            set status="RESET"
                            where dataset_id=1871 
                            and grid_queue_id in (%s) """%(r_))

else:
    print "No cleaning needed"

# checking for condor jobs in processing status
#print "checking jobs with status mismatch"
#os.system("""condor_q -r i3filter  -format "\"%s\",\n" ClusterId | grep -v ? > tmpRunningJobs""")

#f_a = open("tmpRunningJobs")
#l_a = f_a.readlines()
##print l_a
#l_a = l_a[1:-1]
#GQId = ""
#for i in l_a:
#    GQId+=i.strip("\n")
#    
#GQId = GQId[:-1]
#
#dbInfo = dbs4_.fetchall("""select grid_queue_id from i3filter.job where dataset_id=1870 and status="OK" and grid_queue_id in (%s) """%(GQId))
#
#
#if len(dbInfo):
#    mMatch = []
#    for i in dbInfo: mMatch.append(i[0])
#    
#    print "The following jobs have status mismatch (stuck on condor) :"
#    for m in mMatch:
#        print m
#
#else:
#    print "no jobs with status mismatch (stuck jobs)"