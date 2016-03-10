"""
Custom argument parser with preset loglevel arguments
"""

import argparse

def get_defaultparser(scriptdoc=__doc__):
    """
    Get an argparser.ArgumentParser 

    Returns (argparse.ArgumentParser) : Preconfigured Argumentparser
                                         with loglevel and debug entries

    """
    parser = argparse.ArgumentParser(description=scriptdoc)
    parser.add_argument('--loglevel',nargs="?", help="Set loglevel, 10: debug, 20: info, 30: warn",type=int,dest="loglevel",default=20)
    parser.add_argument('--debug', help="Set loglevel to debug",dest="loglevel",action="store_const",const=10)  
    return parser
      

