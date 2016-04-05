
import os
import sys
from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir, get_tmpdir
import libs.times
from icecube import icetray, dataio, dataclasses
from I3Tray import *

import gzip
import shutil

sys.path.append("/data/user/i3filter/SQLServers_n_Clients/")
import SQLClient_dbs4 as dbs4

dbs4_ = dbs4.MySQL()



def FixGoodRunTimes(frame, goodstart, goodend):
    frame['GoodRunStartTime'] = goodstart
    frame['GoodRunEndTime'] = goodend

def FixGcd(logger, file, goodstart, goodend, dryrun):
    # Rename existing GCD file
    if not dryrun:
        infile = "%s/leap_second_affected_%s"%(os.path.dirname(file), os.path.basename(file))
    else:
        infile = file

    # Set filename of new GCD
    if not dryrun:
        outfile = file
    else:
        outfile = "%s/%s"%(get_tmpdir(), os.path.basename(file))

    # Remove .gz from path
    ext = os.path.splitext(outfile)
    if ext[1] != '.gz':
        logger.error("'.gz' extension expected for outfile = %s"%outfile)
        return
    else:
        outfile = ext[0]

    if not dryrun:
        os.rename(file, infile)

    logger.info("The output will be written to: %s"%outfile)

    # Update time
    tray = I3Tray()
    tray.Add('I3Reader', 'GCDReader', filename = infile)
    
    tray.Add("Delete", keys = ['GoodRunStartTime', 'GoodRunEndTime'])

    tray.Add(FixGoodRunTimes,'FixGoodRunTimes',             
                   Streams = [icetray.I3Frame.DetectorStatus],
                   goodstart = goodstart,
                   goodend = goodend)
    
    tray.Add('I3Writer', 'GCDWriter',
        FileName = outfile,
        Streams = [ icetray.I3Frame.Geometry, # ! Only write the GCD frames
                    icetray.I3Frame.Calibration,
                    icetray.I3Frame.DetectorStatus ]
        )
    
    tray.Execute()

    tray.Finish()

    # Check file sizes
    # first gunzip, then compare file size

    # tmp gunzip file
    tmpinfile = "%s/%s"%(get_tmpdir(), 'infile_' + os.path.splitext(os.path.basename(infile))[0])
    with open(tmpinfile, 'wb') as f_out, gzip.open(infile, 'rb') as f_in:
        shutil.copyfileobj(f_in, f_out)

    logger.info("gunzipped infile to %s"%tmpinfile)

    in_size = os.path.getsize(tmpinfile)
    out_size = os.path.getsize(outfile)

    logger.info("The file sizes are: Infile (%s), Outfile(%s)"%(in_size, out_size))

    if in_size != out_size:
        logger.error('Input file size is not equal with output file size')
    else:
        logger.info('Filesize check passed')
        with open(outfile, 'rb') as f_in, gzip.open("%s.gz"%outfile, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    # TODO: Delete tmpinfile, delete outfile (since no .gz)
    os.remove(tmpinfile)
    os.remove(outfile)
    

if __name__ == "__main__":
    parser = get_defaultparser(__doc__,dryrun=True)

    parser.add_argument("-s", "--startrun", type=int, required = True, default=-1,
                      dest="STARTRUN", help="Start fixing GCD files from this run")
    
    parser.add_argument("-e", "--endrun", type=int, required = True, default=-1,
                      dest="ENDRUN", help="End fixing GCD files at this run")
    
    args = parser.parse_args()
    LOGFILE=os.path.join(get_logdir(), 'FixLeapSecondGCDs_')
    logger = get_logger(args.loglevel,LOGFILE)

    if args.STARTRUN > args.ENDRUN:
        logger.critical("Strart run %s must be smaller or equal to endrun %s"%(args.STARTRUN, args.ENDRUN))
        exit(1)

    sql = """SELECT snapshot_id, production_version, run_id, good_tstart, good_tstart_frac, good_tstop, good_tstop_frac
            FROM grl_snapshot_info 
            WHERE run_id BETWEEN %s AND %s"""%(args.STARTRUN, args.ENDRUN)

    info = dbs4_.fetchall(sql, UseDict = True)
   
    for run in info:
        logger.info("Fix GCD for run %s"%run['run_id'])

        date = run['good_tstart']

        gcdfile = "/data/exp/IceCube/%s/filtered/level2/OfflinePreChecks/DataFiles/%s%s/Level2_IC86.2015_data_Run%s_%s_%s_GCD.i3.gz"%(
                    str(date.year), str(date.month).zfill(2), str(date.day).zfill(2),
                      str(run['run_id']).zfill(8), run['production_version'], run['snapshot_id'])

        logger.info("Fix GCD %s"%gcdfile)

        if not os.path.isfile(gcdfile):
            logger.error("GCD file doesn't exist: %s"%gcdfile)
        else:
            start_ns = libs.times.ComputeTenthOfNanosec(run['good_tstart'], run['good_tstart_frac'])
            stop_ns = libs.times.ComputeTenthOfNanosec(run['good_tstop'], run['good_tstop_frac'])
            
            start = dataclasses.I3Time(run['good_tstart'].year, start_ns)
            stop = dataclasses.I3Time(run['good_tstop'].year, stop_ns)
            
            FixGcd(logger, gcdfile, start, stop, args.dryrun)
