"""
Convenience function to provide easy logging
"""

import logging
import os.path
from datetime import datetime
import sys
from svn import SVN

# add logtime
LOGFORMAT = '[%(asctime)s] %(levelname)s: %(module)s(%(lineno)d):   %(message)s'

class DummyLogger(object):
    """
    Emulate logging.Logger
    """

    def __init__(self,*args,**kwargs):
        #self = logging.getLogger("dummy")
        pass

    def debug(self,text):
        print text

    def error(self,text):
        print text
    
    def info(self,text):
        print text

    def warning(self,text):
        print text
    
    def log(self,text):
        print text

    def exception(self,text):
        print text

    def critical(self,text):
        print text



def get_logger(loglevel,logfile):
    """
    A root logger with a formatted output logging to stdout and a file

    Args:
        loglevel (int): 10,20,30,... the higher the less logging
        logfile (str): write logging to this file as well as stdout

    """   

    from files import get_rootdir
    
    def exception_handler(exctype, value, tb):
        logger.critical("Uncaught exception", exc_info=(exctype, value, tb))

    logger = logging.getLogger()
    logger.setLevel(loglevel)
    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    formatter = logging.Formatter(LOGFORMAT)
    ch.setFormatter(formatter)
    today = datetime.now()
    today = today.strftime("%Y_%m_%d_%H_%M")
    logend = ".log"
    if "--dryrun" in sys.argv[1:]:
        logend = ".DRYRUN.log"
    if logfile.endswith(".log"):
        logfile.replace(".log",today+logend)
    else:
        logfile += (today + logend)
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
    logger.info("Using Python %s" % sys.version.replace('\n', ' '))

    svn = SVN(get_rootdir(), logger)

    logger.info("SVN Revision %s" % svn.get('Revision'))
    return logger


