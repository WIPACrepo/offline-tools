"""
Convenience function to provide easy logging
"""

import logging
import os.path
from datetime import datetime
import sys

# add logtime
LOGFORMAT = '[%(asctime)s] %(levelname)s: %(module)s(%(lineno)d):   %(message)s'
#alertstring = lambda x :  "\033[0;31m" + x + "\033[00m"

def get_logger(loglevel,logfile):
    """
    A root logger with a formatted output logging to stdout and a file

    Args:
        loglevel (int): 10,20,30,... the higher the less logging
        logfile (str): write logging to this file as well as stdout

    """   
    
    def exception_handler(exctype, value, tb):
        logger.error("Uncaught exception", exc_info=(exctype, value, tb))

    logger = logging.getLogger()
    logger.setLevel(loglevel)
    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    formatter = logging.Formatter(LOGFORMAT)
    ch.setFormatter(formatter)
    today = datetime.now()
    today = today.strftime("%Y-%m-%d_%H-%M")
    if logfile.endswith(".log"):
        logfile.replace(".log",today+".log")
    else:
        logfile += (today + ".log")
    logfilecount = 1
    while os.path.exists(logfile):
        logfile = logfile.replace("." + str(logfilecount -1),"")
        logfile = logfile +"." + str(logfilecount)
        logfilecount += 1
        if logfilecount >= 60:
            raise SystemError("More than 1 logfile per second, this is insane.. aborting")
    fh = logging.FileHandler(logfile)
    fh.setFormatter(formatter)
    fh.setLevel(loglevel)
    logger.addHandler(ch)
    logger.addHandler(fh)
    sys.excepthook = exception_handler
    firstlog = " ".join(sys.argv)
    logger.info("Starting " + firstlog)
    return logger

