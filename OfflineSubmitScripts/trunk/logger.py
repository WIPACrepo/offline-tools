"""
Convenience function to provide easy logging
"""

import logging
from datetime import datetime

LOGFORMAT = '%(levelname)s:%(message)s:%(module)s:%(funcName)s:%(lineno)d'
#alertstring = lambda x :  "\033[0;31m" + x + "\033[00m"

def GetLogger(loglevel,logfile):
    """
    A root logger with a formatted output logging to stdout and a file

    Args:
        loglevel (int): 10,20,30,... the higher the less logging
        logfile (str): write logging to this file as well as stdout

    """   
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
    fh = logging.FileHandler(logfile)
    fh.setFormatter(formatter)
    fh.setLevel(loglevel)
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

Logger = GetLogger(20,"testlog")
Logger.warn("This is is test")
