import os
import sys

from I3Tray import *
from icecube import icetray, dataclasses, dataio, hdfwriter

from icecube import filter_tools
from icecube.filterscripts import filter_globals
from icecube.filterscripts.onlinel2filter import OnlineL2Filter
from icecube.filterscripts.gfufilter import GammaFollowUp
from icecube.phys_services.which_split import which_split

from datetime import datetime
import subprocess

from libs.logger import get_logger
from libs.argparser import get_defaultparser
from libs.files import get_logdir
from libs.databaseconnection import DatabaseConnection

# parse command-line arguments
parser = get_defaultparser(__doc__, dryrun = True)
parser.add_argument('-o', required = False, default = None, type = str, help = 'Output filename. If no output file has been specified, the output name will be a tmp file. If the tmp file has been validated succesfullyy, the input file gets replaced by the output/tmp file.')
parser.add_argument('-i', type = str, help = 'Input pass2 L2 file')
parser.add_argument('-g', type = str, help = 'Input pass2 L2 GCD file')
parser.add_argument("--npx", action = "store_true", default = False, help = "Use this option if you let run this script on NPX")
args = parser.parse_args()

LOGFILE = os.path.join(get_logdir(sublogpath = 'PostProcessing'), 'PostProcessing_FixGFU_')
logger = get_logger(args.loglevel, LOGFILE, args.npx)

start = datetime.now()

# Input files
inputfiles = [args.g, args.i]
logger.info('Input files: {}'.format(inputfiles))

if not args.i.endswith('.zst'):
    raise RuntimeError('Can only handle .zst files properly')

# define output
outputfile = args.o

if outputfile is None:
    counter = 0

    while True:
        outputfile = os.path.join(os.path.dirname(args.i), 'FixGFU_{}_'.format(counter) + os.path.basename(args.i))
        if not os.path.exists(outputfile):
            break

        counter += 1
        if counter > 20:
            raise RuntimeError('Could not find a tmp file name. Last tested name: {}'.format(outputfile))


logger.info('Output file: {}'.format(outputfile))

# Find run data
run_id = int(args.i.split('Run00')[1].split('_')[0])
file_id = int(args.i.split('Subrun00000000_00')[1].split('.')[0])

logger.info('File metadata: run_id = {0}, file_id = {1}'.format(run_id, file_id))

# Check if post processing has already been performed
db = DatabaseConnection.get_connection('filter-db', logger)

sql = "SELECT * FROM i3filter.pass2_gfu_post_processing WHERE run_id = {0} AND sub_run = {1} AND done = 1".format(run_id, file_id)
rows = db.fetchall(sql, UseDict = True)
if len(rows):
    logger.info('This file has already been GFU post processed.')
    logger.info('DB response: {}'.format(rows))
    logger.info('Exit')
    exit()

# configure logging
icetray.I3Logger.global_logger.set_level(icetray.I3LogLevel.LOG_INFO)
icetray.I3Logger.global_logger.set_level_for_unit('I3FilterModule',        icetray.I3LogLevel.LOG_INFO)
icetray.I3Logger.global_logger.set_level_for_unit('I3ResponseMapSplitter', icetray.I3LogLevel.LOG_WARN)
icetray.I3Logger.global_logger.set_level_for_unit('lilliput',              icetray.I3LogLevel.LOG_WARN)


##############################
#  ICETRAY PROCESSING BELOW  #
##############################

# correct all frames, which passed the OnlineL2Filter
def IsOnlineL2Event(f):
    return which_split(split_name=filter_globals.InIceSplitter)(f) \
       and f.Has('FilterMask') \
       and f['FilterMask']['OnlineL2Filter_17'].condition_passed


# build tray
tray = I3Tray()

tray.context['I3FileStager'] = dataio.get_stagers()
tray.Add('I3Reader', FilenameList=inputfiles)

# copy muon filter result to frame (needed by OnlineL2Filter)
def MuonFilterToFrame(frame):
    passed = frame['FilterMask']['MuonFilter_13'].condition_passed
    frame['MuonFilter_13'] = icetray.I3Bool(passed)
    return True

tray.Add(MuonFilterToFrame, If=IsOnlineL2Event)

# restore CleanedMuonPulses
tray.Add('Copy', Keys=['OnlineL2_CleanedMuonPulses', filter_globals.CleanedMuonPulses],
         If=lambda f: IsOnlineL2Event(f) and not f.Has(filter_globals.CleanedMuonPulses))

# clear old/existing OnlineL2/GFU reco's from the frame
def OnlineL2Cleaner(frame):
    for key in frame.keys():
        if key.startswith('OnlineL2') or key.startswith('GFU') or key.startswith('GammaFollowUp'):
            frame.Delete(key)
    return True

tray.Add(OnlineL2Cleaner, If=IsOnlineL2Event)

# add first pulses (needed by OnlineL2Filter)
icetray.load("DomTools", False)
tray.AddModule("I3FirstPulsifier", "BaseProc_first-pulsify",
               InputPulseSeriesMapName = filter_globals.CleanedMuonPulses,
               OutputPulseSeriesMapName = 'FirstPulseMuonPulses',
               KeepOnlyFirstCharge = False,   # default
               UseMask = False,               # default
               If = lambda f: IsOnlineL2Event(f) and not f.Has('FirstPulseMuonPulses')
              )

# run OnlineL2 filter
SplineDir = "/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines/"
tray.AddSegment(OnlineL2Filter, "OnlineL2",
                pulses                   = filter_globals.CleanedMuonPulses,
                linefit_name             = filter_globals.muon_linefit,
                llhfit_name              = filter_globals.muon_llhfit,
                SplineRecoAmplitudeTable = os.path.join(SplineDir, 'InfBareMu_mie_abs_z20a10_V2.fits'),
                SplineRecoTimingTable    = os.path.join(SplineDir, 'InfBareMu_mie_prob_z20a10_V2.fits'),
                forceOnlineL2BadDOMList  = 'BadDomsList',
                If = IsOnlineL2Event)

# run GFU filter
tray.AddSegment(GammaFollowUp, "GammaFollowUp",
                pulses              = filter_globals.CleanedMuonPulses,
                OnlineL2SegmentName = "OnlineL2",
                angular_error       = True,
                If = lambda f: IsOnlineL2Event(f) and f['OnlineL2Filter_17'].value)

# Update FilterMask with new GFU filter result
def UpdateFilterMask(f):
    f['FilterMask']['GFUFilter_17'].condition_passed = f.Has('GFUFilter_17') and f['GFUFilter_17'].value
    return True

tray.Add(UpdateFilterMask, If=IsOnlineL2Event)

# Cleanup temporarily created keys
tray.AddModule('Delete', Keys=[ 'MuonFilter_13',
                                'OnlineL2Filter_17',
                                'GFUFilter_17',
                                'FirstPulseMuonPulses',
                                filter_globals.CleanedMuonPulses ],
                         If=IsOnlineL2Event)

# write to i3 file
tray.AddModule("I3Writer", filename=outputfile,
        Streams=[icetray.I3Frame.TrayInfo, icetray.I3Frame.DAQ, icetray.I3Frame.Physics],
        )

tray.Add('TrashCan')

tray.Execute()

logger.info('File created')
logger.info('Start validation')

try:
    subprocess.check_output(['/cvmfs/icecube.opensciencegrid.org/py2-v3/RHEL_6_x86_64/bin/zstd', '--test', outputfile])
except sub.CalledProcessError as e:
    logger.error('File {0} is corrupted: {1}'.format(outputfile, str(e)))
    raise RuntimeError('File {0} is corrupted: {1}'.format(outputfile, str(e)))

logger.info('No stream errors found')

logger.info('Check filesize (new file should be larger)')

outsize = os.path.getsize(outputfile)
insize = os.path.getsize(args.i)

logger.debug('Input filesize  = {}'.format(insize))
logger.debug('Output filesize = {}'.format(outsize))

if outsize < insize:
    logger.error('Output file is smaller than input file.')
    logger.error('Abort.')
else:
    logger.info('Filesize check passed')

    if args.o is None:
        logger.info('Replace old file with new file')
    
        logger.info('remove {}'.format(args.i))
        if not args.dryrun:
            os.remove(args.i)
    
        logger.info('rename {0} -> {1}'.format(outputfile, args.i))
        if not args.dryrun:
            os.rename(outputfile, args.i)
    
    sql = 'INSERT INTO i3filter.pass2_gfu_post_processing (`run_id`, `sub_run`, `path`, `done`, `date`) VALUES ({run_id}, {sub_run}, \'{path}\', 1, NOW())'.format(run_id = run_id, sub_run = file_id, path = args.i)
    
    logger.debug('SQL: {}'.format(sql))
    
    if not args.dryrun:
        db.execute(sql)
    
    logger.info('Post processing L2 gix GFU done')

time = (datetime.now() - start).total_seconds()

logger.info('Execution time: {}s'.format(time))

