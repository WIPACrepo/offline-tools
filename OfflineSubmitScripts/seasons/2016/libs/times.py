"""
Take care of leap seconds
"""

from ftplib import FTP
import datetime
import re
import os.path

           
def ComputeTenthOfNanosec(time_, time_frac, leap_second_file, logger):
    """
    Calculate time passed since midnight of this years new years eve in 
    100 picoseconds precision

    Args:
        time_ (datetime): the time to be converted
        time_frac (int): ten-digit 100 picosecond precise time (in 100 picsec)
        leap_second_file (string): Path to file
        logger (logging.Logger): The logger

    """
    # use leap second-aware version
    return seconds_passed_since_newyears(time_, leap_second_file = leap_second_file, logger = logger)*10000000000 + int(time_frac)

###########################################

def download_leapseconds_list(leap_second_file, logger):
    """
    Download a leapseconds file from our favorite server

    Args:
        leap_second_file (string): Path to file
        logger (logging.Logger): The logger
    """

    ftp = FTP('time.nist.gov')     # connect to host, default port
    ftp.login()    
    ftp.cwd("pub")
    ftp.retrbinary('RETR leap-seconds.list', open(leap_second_file, 'wb').write)

############################################

def check_leapseconds_list_valid(leap_second_file, logger):
    """
    check if the leapseconds file is expired
    
    Args:
        leap_second_file (string): Path to file
        logger (logging.Logger): The logger

    Returns (bool)
    """
    pattern = "File expires on:\s*(?P<day>[0-9]+)\s*(?P<month>[A-Za-z]*)\s*(?P<year>[0-9]{4})" 
    pattern = re.compile(pattern)

    try:
        with open(leap_second_file) as f:
            data = f.read()
    except IOError:
        return False

    expires = pattern.search(data).groupdict()
    expires["day"] = expires["day"].zfill(2)
    expires = datetime.datetime.strptime("{day}_{month}_{year}".format(**expires),"%d_%B_%Y")
    return expires > datetime.datetime.now()

##########################################

def get_leapsecond_years(leap_second_file, logger):
    """
    parse the leapseconds file for years with leapseconds

    Args:
        leap_second_file (string): Path to file
        logger (logging.Logger): The logger
    """
    pattern = "[0-9]{10}\s*[0-9]{2}\s*\#\s*(?P<day>[0-9]+)(?P<monthyear>.*)"
    pattern = re.compile(pattern)
    dates = []
    if not os.path.isfile(leap_second_file):
        logger.warning("No leap seconds file (%s) found, attempting to download..." % leap_second_file)
        download_leapseconds_list(leap_second_file, logger = logger)

    if not check_leapseconds_list_valid(leap_second_file, logger = logger):
        logger.warning("Leapseconds list (%s) expired, downlowding a new one..." % leap_second_file)
        download_leapseconds_list(leap_second_file, logger = logger)

    with open(leap_second_file) as f:
        for line in f.xreadlines():
            result = pattern.search(line)
            if result is not None:
                result = result.groupdict()
                result["day"] = result["day"].zfill(2)
                date = "{day}{monthyear}".format(**result)
                dates.append(date)

    dates = map(lambda x: datetime.datetime.strptime(x,"%d %b %Y"),dates)
    return dates

#########################################

def has_leapsecond(moment, leap_second_file, logger):
    """
    Tell if a leapsecond has been assigned to this time

    Args:
        moment (datetime.datetime): The datetime that should be checked
        leap_second_file (string): Path to file
        logger (logging.Logger): The logger
    """
    
    # check if this year has a leapsecond
    leapy = filter(lambda x: x.year == moment.year,get_leapsecond_years(leap_second_file, logger))
    if leapy:
        # check if moment is after the switch
        return moment > leapy[0]
    
    return False    

##########################################

def seconds_passed_since_newyears(moment, leap_second_file, logger):
    """
    Calculates how many seconds have been passed since
    00:00:00 01/01/thisyear

    Args:
        moment (datetime.datetime): translate this in seconds
        leap_second_file (string): Path to file
        logger (logging.Logger): The logger

    Returns (int): seconds
    """

    non_leap_seconds = ((datetime.date(int(moment.year),int(moment.month),int(moment.day)) - \
                    datetime.date(int(moment.year),1,1)).days * 86400 + \
                    int(moment.hour) * 3600 + \
                    int(moment.minute)  * 60 + \
                    int(moment.second))
    
    return (non_leap_seconds + has_leapsecond(moment, leap_second_file, logger))

##########################################

if __name__ == "__main__":
    from logger import DummyLogger

    logger = DummyLogger()

    file = 'leap-seconds.list'

    print check_leapseconds_list_valid(leap_second_file = file, logger = logger)

    momentA = datetime.datetime(2015,11,11)
    momentB = datetime.datetime(2015,2,1)
    momentC = datetime.datetime(2014,11,11)
    print has_leapsecond(momentA, leap_second_file = file, logger = logger)
    print has_leapsecond(momentB, leap_second_file = file, logger = logger)
    print has_leapsecond(momentC, leap_second_file = file, logger = logger)
    print seconds_passed_since_newyears(momentA, leap_second_file = file, logger = logger)
    print seconds_passed_since_newyears(momentC, leap_second_file = file, logger = logger)
    print ComputeTenthOfNanosec(momentA, 1234567890, leap_second_file = file, logger = logger)
    print ComputeTenthOfNanosec(momentC, 1234567890, leap_second_file = file, logger = logger)

