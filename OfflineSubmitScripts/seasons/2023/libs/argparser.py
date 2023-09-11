"""
Custom argument parser with preset loglevel arguments
"""

import argparse

def get_defaultparser(doc, dryrun = False, logfile = True):
    """
    Get an argparser.ArgumentParser 

    Args:
        doc (str): __doc__ of current module

    Returns (argparse.ArgumentParser) : Preconfigured Argumentparser
                                         with loglevel and debug entries

    """
    parser = argparse.ArgumentParser(description = doc)
    parser.add_argument('--loglevel', nargs = "?", help = "Set loglevel, 10: debug, 20: info, 30: warn", dest = "loglevel", type = int, default = 20)
    parser.add_argument('--debug', help = "Set loglevel to debug", dest = "loglevel", action = "store_const", const = 10)

    if logfile:
        parser.add_argument('--logfile', help = "Defines log file path. If not set, default value is used. Usually, the default is fine.", type = str, default = None, required = False)

    if dryrun: 
        parser.add_argument('--dryrun', help="Just pretending. Don't do actual work.", action = "store_true", default = False)

    return parser
