"""
Export a dummylogger class to ease the transition from print -> python.logging
"""

#import logging

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

