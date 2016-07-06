#!/usr/bin/env python

import sys
sys.path.append("/data/user/i3filter/SQLServers_n_Clients/")
sys.path.append('/data/user/i3filter/IC86_OfflineProcessing/OfflineSubmitScripts_2015/')

from libs.logger import get_logger
from libs.argparser import get_defaultparser

from icecube import dataio, icetray, dataclasses

import os

import SQLClient_dbs4 as dbs4

import urllib
import urllib2 as u
import json

# POST data
auth_payload = "user=icecube&pass=skua"
# URL with placeholder for run_num
url_pat = "https://hercules.icecube.wisc.edu/run_doms/%s/" 

def get_grl_info(run_number, logger):
    req = u.Request('https://live.icecube.wisc.edu/snapshot-export/', urllib.urlencode({'user': 'icecube', 'pass': 'skua', 'start_run': run_number, 'end_run': run_number}))
    response = u.urlopen(req).read()
    d = json.loads(response)

    if len(d['runs']) != 1:
        raise

    return d['runs'][0]

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

    with open("run_%s.txt" % run_number, 'w') as f:
        f.write(resp_text)

    run_doms = json.loads(resp_text)

    GRL = get_grl_info(run_number, logger)

    bdlist = []
    unconf_doms = []
    dropped_doms = []
    problem_doms = []

    for key, om in run_doms['unconfigured_doms'].iteritems():
        bdlist.append(icetray.OMKey(om['string'], om['position']))
        unconf_doms.append(icetray.OMKey(om['string'], om['position']))

    logger.debug("unconfigured_doms: %s"%len(run_doms['unconfigured_doms']))
   
    counter = 0

    print "start time: %s; end time: %s" % (GRL['good_tstart'], GRL['good_tstop'])

    for key, src in run_doms['dropped_doms'].iteritems():
        for om in src:
            print "if %s >= %s and %s <= %s:" % ( om['drop_time'], GRL['good_tstart'], om['drop_time'], GRL['good_tstop'])
            if om['drop_time'] >= GRL['good_tstart'] and om['drop_time'] <= GRL['good_tstop']:
                counter = counter + 1
                bdlist.append(icetray.OMKey(om['dom_string'], om['dom_position']))
                dropped_doms.append(icetray.OMKey(om['dom_string'], om['dom_position']))
    
    logger.info("dropped_doms: %s"%counter)

    counter = 0
    for type in ['No HV']:
        for key, om in run_doms['problem_doms'][type].iteritems():
            counter = counter + 1
            bdlist.append(icetray.OMKey(om['string'], om['position']))
            problem_doms.append(icetray.OMKey(om['string'], om['position']))

    logger.debug("problem_doms: %s"%counter)

    bdlist = [dom for dom in bdlist if dom.string > 0]

    sorted(bdlist)
    sorted(unconf_doms)
    sorted(dropped_doms)
    sorted(problem_doms)

    return bdlist, set(unconf_doms), set(dropped_doms), set(problem_doms)

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

    sorted(bdl)

    logger.debug("GCD bdl:")
    for om in bdl:
        logger.debug(om)

    return [om for om in bdl]

def get_disabled_oms_from_gcd(filename, logger):
    file = dataio.I3File(filename)
    g = file.pop_frame()
    c = file.pop_frame()
    d = file.pop_frame()

    disabled_doms = []
    goodSlcOnlyKeys = []

    for dom in d['I3Geometry'].omgeo.keys():
        if (dom not in d['I3DetectorStatus'].dom_status.keys() or d['I3DetectorStatus'].dom_status[dom].pmt_hv == 0) and dom.string > 0:
            disabled_doms.append(dom)

        if dom in d['I3DetectorStatus'].dom_status.keys() and d['I3DetectorStatus'].dom_status[dom].lc_mode == d['I3DetectorStatus'].dom_status[dom].LCMode.SoftLC and d['I3DetectorStatus'].dom_status[dom].pmt_hv > 0 and dom.string > 0:
            goodSlcOnlyKeys.append(dom)

    return set(disabled_doms), set(goodSlcOnlyKeys)

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


        logger.info("=== Run %s ================================" % run['run_id'])
        logger.info("GCD file: %s"%gcdfile)

        if not os.path.isfile(gcdfile):
            logger.error("GCD file doesn't exist")
            continue

        gcd_bdlist = get_bd_list_from_gcd(gcdfile, logger)

        gcd_unconf_doms, goodSlcOnlyKeys = get_disabled_oms_from_gcd(gcdfile, logger)

        if gcd_bdlist == None:
            continue

        live_bdlist, live_unconf_doms, live_dropped_doms, live_problem_doms = get_bd_list_from_i3live(run['run_id'], logger)

        gcd_bdlist = set(gcd_bdlist)
        live_bdlist = set(live_bdlist)

        logger.info("%s bad doms in GCD found; %s from live"%(len(gcd_bdlist), len(live_bdlist)))
        logger.info("%s only SLC doms" % len(goodSlcOnlyKeys))

        final_live = live_bdlist.union(goodSlcOnlyKeys)
        if len(final_live) == len(gcd_bdlist) and len(gcd_bdlist) == len(gcd_bdlist & final_live):
            logger.info("GCD and i3live bd lists are equal")
        else:
            logger.error("GCD and i3live bd lists are not equal")
            diff = gcd_bdlist - final_live
            diff = list(diff) + list(final_live - gcd_bdlist)

            for om in sorted(diff):
                s = "Difference dom: %s -> InGCD = %s, I3LUnconf = %s, I3LDropped = %s, I3LProblem = %s" % (om, om in gcd_bdlist, om in live_unconf_doms, om in live_dropped_doms, om in live_problem_doms)

                logger.error(s)

        intersection = sorted(gcd_bdlist & live_bdlist)
        difference = sorted(gcd_bdlist - live_bdlist)
        difference2 = sorted(live_bdlist - gcd_bdlist)

        logger.debug('===============================================')
        logger.debug("Common DOMs (%s):"%len(intersection))
        for dom in intersection:
            logger.debug(dom)

        logger.debug('===============================================')
        logger.debug("GCD - live DOMs (%s):"%len(difference))
        for dom in difference:
            logger.debug(dom)

        logger.debug('===============================================')
        logger.debug("live - GCD DOMs (%s):"%len(difference2))
        for dom in difference2:
            logger.debug(dom)

        icetop = [om for om in live_bdlist if om.om > 64]

        logger.debug('===============================================')
        logger.debug("IceTop OMs in i3live: %s"%len(icetop))
        for dom in icetop:
            logger.debug(dom)

        unconf_intersection = sorted(live_unconf_doms & gcd_unconf_doms)
        unconf_diff = sorted(gcd_unconf_doms - live_unconf_doms)
        unconf_diff2 = sorted(live_unconf_doms - gcd_unconf_doms)

        logger.debug('===============================================')
        logger.debug("Unconfigured doms: live (%s), GCD (%s)" % (len(live_unconf_doms), len(gcd_unconf_doms)))
        logger.debug("Common doms: %s" % len(unconf_intersection))
        for dom in unconf_intersection:
            logger.debug(dom)

        logger.debug('===============================================')
        logger.debug("GCD - live: %s" % len(unconf_diff))
        for dom in unconf_diff:
            logger.debug(dom)

        logger.debug('===============================================')
        logger.debug("live - GCD: %s" % len(unconf_diff2))
        for dom in unconf_diff2:
            logger.debug(dom)

        difference = set(difference)

        missing_live_doms = sorted(goodSlcOnlyKeys & difference)
        missing_live_doms_diff = sorted(difference - goodSlcOnlyKeys)
        missing_live_doms_diff2 = sorted(goodSlcOnlyKeys - difference)

        logger.debug('===============================================')
        logger.debug("goodSlcOnlyKeys in GCD: %s" % len(goodSlcOnlyKeys))
        for dom in goodSlcOnlyKeys:
            logger.debug(dom)

        logger.debug('===============================================')
        logger.debug("Missing live doms: %s, goodSlcOnlyKeys %s" % (len(difference), len(goodSlcOnlyKeys)))
        for dom in missing_live_doms:
            logger.debug(dom)
        
        logger.debug('===============================================')
        logger.debug("missing - goodSlcOnlyKeys: %s" % len(missing_live_doms_diff))
        for dom in missing_live_doms_diff:
            logger.debug(dom)
        
        logger.debug('===============================================')
        logger.debug("goodSlcOnlyKeys - missing: %s" % len(missing_live_doms_diff2))
        for dom in missing_live_doms_diff2:
            logger.debug(dom)
        
        

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

