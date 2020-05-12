#!/usr/bin/env python

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from libs.config import get_config
from libs.logger import DummyLogger

config = get_config(DummyLogger())

for sec in config.sections():
    print ("declare -A %s" % (sec))
    for key in config[sec]:
        print ('%s[%s]="%s"' % (sec, key, config[sec][key]))
