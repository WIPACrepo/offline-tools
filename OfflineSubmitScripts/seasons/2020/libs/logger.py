"""
Convenience function to provide easy logging
"""

import logging
import os
from datetime import datetime
import sys
#from svn import SVN
from .config import get_config

class DummyLogger(object):
    """
    Emulate logging.Logger
    """

    def __init__(self, *args, **kwargs):
        self.silence = False
        self.level = 20

    def debug(self, text):
        if not self.silence and self.level <= 10:
            print (text)

    def error(self, text):
        if not self.silence and self.level <= 40:
            print (text)
    
    def info(self, text):
        if not self.silence and self.level <= 20:
            print (text)

    def warning(self, text):
        if not self.silence and self.level <= 30:
            print (text)
    
    def log(self, level, text):
        if not self.silence and self.level <= level:
            print (text)

    def exception(self, text):
        self.error(text)

    def critical(self, text):
        if not self.silence and self.level <= 50:
            print (text)

    def set_level(self, level):
        self.level = level

def get_logger(loglevel, logfile, svn_info_from_file = False):
    """
    A root logger with a formatted output logging to stdout and a file

    Args:
        loglevel (int): 10,20,30,... the higher the less logging
        logfile (str): write logging to this file as well as stdout
        svn_info_from_file (bool): Load SVN info from file (e.g. if SVN is not available on the machine)
    """

    from .path import get_rootdir, get_tmpdir

    # logformat = get_config(DummyLogger()).get('Logger', 'Format')
    logformat = '[%(asctime)s] %(levelname)s: %(module)s(%(lineno)d):   %(message)s'

    def exception_handler(exctype, value, tb):
        logger.critical("Uncaught exception", exc_info=(exctype, value, tb))

    logger = logging.getLogger()
    logger.setLevel(loglevel)
    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    formatter = logging.Formatter(logformat)
    ch.setFormatter(formatter)
    today = datetime.now()
    today = today.strftime("%Y_%m_%d_%H_%M")
    logend = ".log"

    if "--dryrun" in sys.argv[1:]:
        logend = ".DRYRUN.log"

    if logfile.endswith(".log"):
        logfile.replace(".log", today + logend)
    else:
        logfile += (today + logend)

    logfilecount = 1
    while os.path.exists(logfile):
        logfile = logfile.replace("." + str(logfilecount - 1), "")
        logfile = logfile + "." + str(logfilecount)
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
    logger.info("Using Python {0}".format(sys.version.replace('\n', ' ')))

    """
    if svn_info_from_file:
        svn = SVN(get_rootdir(), logger, os.path.join(get_tmpdir(), 'svninfo.txt'))
    else:
        svn = SVN(get_rootdir(), logger)

    logger.info("SVN Revision {0}".format(svn.get('Revision')))
    """

    return logger

def delete_log_file(logger):
    """
    Deletes all logging files of an logger and removes those filehandlers from the logger.

    Args:
        logger (logging.Logger): The logger
    
    Returns:
        list: List of the removed FileHandlers
    """

    filehandlers = [handler for handler in logger.handlers if isinstance(handler, logging.FileHandler)]

    logger.debug("Found {0} file handlers".format(len(filehandlers)))

    for h in filehandlers:
        logger.removeHandler(h)
    
    logger.warning("Removed {0} file handlers from logger".format(len(filehandlers)))

    for h in filehandlers:
        os.remove(h.baseFilename)
        logger.warning("Deleted {0}".format(h.baseFilename))

    return filehandlers

