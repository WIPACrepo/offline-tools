"""
Take care of leap seconds
"""

from ftplib import FTP
import datetime
import re
import os.path

def download_leapseconds_list():
    """
    Download a leapseconds file from our favorite server
    """

    ftp = FTP('time.nist.gov')     # connect to host, default port
    ftp.login()    
    ftp.cwd("pub")
    ftp.retrbinary('RETR leap-seconds.list', open('leap-seconds.list', 'wb').write)

############################################

def check_leapseconds_list_valid():
    """
    check if the leapseconds file is expired
    
    Returns (bool)
    """
    pattern = "File expires on:\s*(?P<day>[0-9]+)\s*(?P<month>[A-Za-z]*)\s*(?P<year>[0-9]{4})" 
    pattern = re.compile(pattern)
    with open("leap-seconds.list") as f:
        data = f.read()
    expires = pattern.search(data).groupdict()
    expires["day"] = expires["day"].zfill(2)
    expires = datetime.datetime.strptime("{day}_{month}_{year}".format(**expires),"%d_%B_%Y")
    return expires > datetime.datetime.now()

##########################################

def get_leapsecond_years():
    """
    parse the leapseconds file for years with leapseconds
    """
    pattern = "[0-9]{10}\s*[0-9]{2}\s*\#\s*(?P<day>[0-9]+)(?P<monthyear>.*)"
    pattern = re.compile(pattern)
    dates = []
    if not os.path.isfile("leap-seconds.list"):
        print "No leap seconds file found, attempting to download..."
        download_leapseconds_list()

    if not check_leapseconds_list_valid():
        print "Leapseconds list expired, downlowding a new one..."
        download_leapseconds_list() 

    with open("leap-seconds.list") as f:
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

def has_leapsecond(moment):
    """
    Tell if a leapsecond has been assigned to this time
    """
    
    # check if this year has a leapsecond
    leapy = filter(lambda x: x.year == moment.year,get_leapsecond_years())
    if leapy:
        # check if moment is after the switch
        return moment > leapy[0]
    
    return False    

##########################################

def seconds_passed_since_newyears(moment):
    """
    Calculates how many seconds have been passed since
    00:00:00 01/01/thisyear

    Args:
        moment (datetime.datetime): translate this in seconds

    Returns (int): seconds
    """

    non_leap_seconds = ((datetime.date(int(moment.year),int(moment.month),int(moment.day)) - \
                    datetime.date(int(moment.year),1,1)).days * 86400 + \
                    int(moment.hour) * 3600 + \
                    int(moment.minute)  * 60 + \
                    int(moment.second)) * 10000000000
    
    return non_leap_seconds + has_leapsecond(moment)

##########################################

if __name__ == "__main__":
    print check_leapseconds_list_valid()

    momentA = datetime.datetime(2015,11,11)
    momentB = datetime.datetime(2015,2,1)
    momentC = datetime.datetime(2014,11,11)
    print has_leapsecond(momentA)
    print has_leapsecond(momentB)
    print has_leapsecond(momentC)
    print seconds_passed_since_newyears(momentA)
    print seconds_passed_since_newyears(momentC)
    

