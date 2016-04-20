#!/usr/bin/env python

import sys
sys.path.append("/data/user/i3filter/SQLServers_n_Clients/")
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/')

from libs.logger import get_logger
from libs.argparser import get_defaultparser

from icecube import dataio, icetray, dataclasses

import os

import SQLClient_dbs4 as dbs4

import urllib2 as u
import json

# POST data
auth_payload = "user=icecube&pass=skua"
# URL with placeholder for run_num
url_pat = "https://hercules.icecube.wisc.edu/run_doms/%s/" 

def get_bd_list_from_i3live(run_number, logger):
    url = url_pat % run_number

    logger.debug("URL: %s"%url)

    # create and send request
    req = u.Request(url, auth_payload)
    try:
        response = u.urlopen(req)
    except u.HTTPError as err:
        # if a 400-500 level status is received, urllib raises exception
        logger.error("Error %s loading URL '%s'." % (err.code, url))
        if err.code == 404:
            logger.error("invalid run number")
        elif err.code == 403:
            logger.error("not authorized")
        raise SystemExit

    resp_text = response.read() 

    run_doms = json.loads(resp_text)

    bdlist = []

    for key, om in run_doms['unconfigured_doms'].iteritems():
        bdlist.append(icetray.OMKey(om['string'], om['position']))

    logger.debug("unconfigured_doms: %s"%len(run_doms['unconfigured_doms']))
   
    counter = 0 
    for key, src in run_doms['dropped_doms'].iteritems():
        for om in src:
            counter = counter + 1
            bdlist.append(icetray.OMKey(om['dom_string'], om['dom_position']))
    
    logger.debug("dropped_doms: %s"%counter)

    counter = 0
    for key, type in run_doms['problem_doms'].iteritems():
        for key, om in type.iteritems():
            counter = counter + 1
            bdlist.append(icetray.OMKey(om['string'], om['position']))

    logger.debug("problem_doms: %s"%counter)
    return bdlist    

def get_bd_list_from_gcd(filename, logger):
    file = dataio.I3File(filename)
    bdl = None
    while file.more():
            gcdframe = file.pop_frame()
            if gcdframe.Has('BadDomsList'):
                    bdl = gcdframe['BadDomsList']
                    break
    if not bdl:
            logger.error('No bad dom list found')

    return [om for om in bdl]

def main(start_run, end_run, dryrun, logger):
    db = dbs4.MySQL()

    sql = """SELECT snapshot_id, production_version, run_id, good_tstart, good_tstart_frac, good_tstop, good_tstop_frac
            FROM grl_snapshot_info 
            WHERE run_id BETWEEN %s AND %s
                AND (good_it = 1 OR good_i3 = 1)"""%(start_run, end_run)

    query = db.fetchall(sql, UseDict = True)

    for run in query:
        date = run['good_tstart']

        gcdfile = "/data/exp/IceCube/%s/filtered/level2/OfflinePreChecks/DataFiles/%s%s/Level2_IC86.2015_data_Run%s_%s_%s_GCD.i3.gz"%(
                    str(date.year), str(date.month).zfill(2), str(date.day).zfill(2),
                      str(run['run_id']).zfill(8), run['production_version'], run['snapshot_id'])


        logger.info("GCD file: %s"%gcdfile)

        if not os.path.isfile(gcdfile):
            logger.error("GCD file doesn't exist")
            continue

        gcd_bdlist = get_bd_list_from_gcd(gcdfile, logger)

        if gcd_bdlist == None:
            continue

        live_bdlist = get_bd_list_from_i3live(run['run_id'], logger)

        gcd_bdlist = set(gcd_bdlist)
        live_bdlist = set(live_bdlist)

        logger.info("%s bad doms in GCD found; %s from live"%(len(gcd_bdlist), len(live_bdlist)))

        intersection = gcd_bdlist & live_bdlist
        difference = gcd_bdlist - live_bdlist
        difference2 = live_bdlist - gcd_bdlist

        logger.info("Common DOMs (%s):"%len(intersection))
        for dom in intersection:
            print dom

        logger.info("GCD - live DOMs (%s):"%len(difference))
        for dom in difference:
            print dom

        logger.info("live - GCD DOMs (%s):"%len(difference2))
        for dom in difference2:
            print dom

        icetop = [om for om in live_bdlist if om.om > 64]

        logger.info("IceTop OMs in i3live: %s"%len(icetop))
        for dom in icetop:
            print dom


if __name__ == '__main__':
    parser = get_defaultparser(__doc__, dryrun = True)

    parser.add_argument("-s", "--startrun", type=int, required = True,
                                      dest="STARTRUN", help="start submission from this run")


    parser.add_argument("-e", "--endrun", type=int, required = True,
                                      dest="ENDRUN", help="end submission at this run")


    args = parser.parse_args()

    LOGFILE=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'I3LiveProblemDomCheck_')
    logger = get_logger(args.loglevel, LOGFILE)

    main(args.STARTRUN, args.ENDRUN, args.dryrun, logger)

