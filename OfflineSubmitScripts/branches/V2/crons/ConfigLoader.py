#!/usr/bin/env python

import sys

sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2016/')
import libs.files
from libs.config import get_config

config = get_config()

for sec in config.sections():
    print "declare -A %s" % (sec)
    for key, val in config.items(sec):
        print '%s[%s]="%s"' % (sec, key, val)
