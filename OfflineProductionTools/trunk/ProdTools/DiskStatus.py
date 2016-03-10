#!/usr/bin/env python

import os,sys
import json
#from collections import namedtuple
import time
import datetime

#_ntuple_diskusage = namedtuple('usage', 'total used free')

def disk_usage(path):
    """Return disk usage statistics about the given path.

    Returned values is a named tuple with attributes 'total', 'used' and
    'free', which are the amount of total, used and free space, in bytes.
    """
    
    toTB = 1024*1024*1024*1024
    
    st = os.statvfs(path)
    free = st.f_bavail * st.f_frsize /toTB
    total = st.f_blocks * st.f_frsize /toTB
    used = (st.f_blocks - st.f_bfree) * st.f_frsize /toTB
    #return _ntuple_diskusage(total, used, free)
    return [total,used,free]


DiskStatus = {}
vols = ["/data/exp","/data/sim","/data/ana"]
for v in vols:
    try:
        DiskStatus[v] = disk_usage(v)
    except Exception, err:
        print "Error: %s"%str(err)

try:
    DayOfWeek = datetime.datetime.now().strftime("%A, ")
    TimeOfDay = time.strftime("%Y-%m-%d %H:%M:%S")
    DiskStatus["LastUpdate"] = DayOfWeek + TimeOfDay

    #TimeOfDay = datetime.datetime.today()
    #TimeOfDay = TimeOfDay.replace(second=0,microsecond=0) 
    #DiskStatus["LastUpdate"] = DayOfWeek + str(TimeOfDay)
except Exception, err:
    print "Error: %s"%str(err)
    


with open('/net/www-i3internal/simulation/ResourceMonitoring/StorageManagement/DiskStatus.json','w') as f:
    #json.dump(DiskStatus,f,ensure_ascii=False)
    json.dump(DiskStatus,f)
    
    
