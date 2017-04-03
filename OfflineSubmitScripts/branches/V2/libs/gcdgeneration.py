
from icecube import icetray

from icecube.gcdserver.Config import get_config as get_server_config
from icecube.gcdserver.I3GeometryBuilder import buildI3Geometry
from icecube.gcdserver.I3CalibrationBuilder import buildI3Calibration
from icecube.gcdserver.I3DetectorStatusBuilder import buildI3DetectorStatus
from icecube.gcdserver.MongoDB import fillBlobDB, getDB
from icecube.gcdserver.I3Live import getLiveRunData
from icecube.gcdserver.util import setStartStopTime

class GCDGenerator(icetray.I3Module):
    def __init__(self, context):
        icetray.I3Module.__init__(self, context)

        self.AddParameter('RunId',
                          'The run id',
                          -1)

        self.AddParameter('I3LiveHost',
                          'I3Live host',
                          get_server_config().get('gcdserver', 'i3live_host'))

    def Configure(self):
        self.run_id = self.GetParameter('RunId')
        self.i3live_host = self.GetParameter('I3LiveHost')

    def Process():
        run_data = getLiverun_data(self.run_id, self.i3live_host)

        if run_data.startTime is None:
            icetray.logging.log_fatal("No run data available for run {0}".format(self.run_id))
            raise Exception("No run data available for run {0}".format(self.run_id))

        blob_db = fillblob_db(getDB(), run = self.run_id, configuration = run_data.configName)

        g = buildI3Geometry(blob_db)
        c = buildI3Calibration(blob_db)
        d = buildI3DetectorStatus(blob_db, run_data)

        setStartStopTime(g, d)
        setStartStopTime(c, d)

        fr = icetray.I3Frame(icetray.I3Frame.Geometry)
        fr['I3Geometry'] = g
        self.push(fr)

        fr = icetray.I3Frame(icetray.I3Frame.Calibration)
        fr['I3Calibration'] = c
        self.push(fr)

        fr = icetray.I3Frame(icetray.I3Frame.DetectorStatus)
        fr['I3DetectorStatus'] = d
        self.push(fr)

def set_production_information(frame, production_version, snapshot_id, good_start_time, good_stop_time):
    frame["Offlineproduction_version"] = dataclasses.I3Double(production_version)
    frame["GRLsnapshot_id"] = dataclasses.I3Double(snapshot_id)
    frame["GoodRungood_start_time"] = good_start_time
    frame["GoodRungood_stop_time"] = good_stop_time

def adjust_dst_time(frame, run, logger):
    """
    Adjust DetectorStatus times when they are way off from the actual start time because of a bug.

    Args:
        frame (I3Frame): The frame
        run (libs.runs.Run): The run
        logger (Logger): The Logger
    """

    if abs((frame['I3DetectorStatus'].start_time.date_time - run.get_start_time().date_time).total_seconds()) > 100:
        logger.warning("DS Start Time Needed Adjustment")
        logger.warning("Original Start Time is: {0}".format(frame['I3DetectorStatus'].start_time.date_time))
        logger.warning("Replaced with Start Time from i3live: {0}".format(run.get_start_time().date_time))
        frame['I3DetectorStatus'].start_time = run.get_start_time()
        
        
    if abs((frame['I3DetectorStatus'].end_time.date_time - run.get_stop_time().date_time).total_seconds()) > 100:
        logger.warning("DS End Time Needed Adjustment")
        logger.warning("Original End Time is: {0}".format(frame['I3DetectorStatus'].end_time.date_time))
        print "Replaced with End Time from i3live: {0}".format(run.get_stop_time())))
        frame['I3DetectorStatus'].end_time = run.get_stop_time()

def generate_gcd(run, gcd_path, spe_correction_file, logger):
    from icecube.BadDomList.BadDomListTraySegment import BadDomList
    from icecube.phys_services.spe_fit_injector import I3SPEFitInjector

    tray = I3Tray()

    trau.Add(GCDGenerator, "GCDGenerator", RunId = run.run_id)
    tray.Add(I3SPEFitInjector, "fixspe", Filename = spe_correction_file)
    tray.Add(set_production_information, "set_production_information",
             good_start_time = run.get_good_start_time(),
             good_stop_time = run.get_good_stop_time(),
             production_version = run.get_production_version(),
             snapshot_id = run.get_snapshot_id(),
             Streams = [icetray.I3Frame.DetectorStatus])
    tray.Add(adjust_dst_time, "adjust_dst_time",
             run = run,
             logger = logger,
             Streams = [icetray.I3Frame.DetectorStatus])
    tray.Add(BadDomList, "baddomlist", RunId = run.run_id)
    tray.Add('I3Writer', 'GCDWriter',
             FileName = gcd_path,
             Streams = [icetray.I3Frame.TrayInfo,
                        icetray.I3Frame.Geometry,
                        icetray.I3Frame.Calibration,
                        icetray.I3Frame.DetectorStatus],
            )
    tray.AddModule("TrashCan","trash")
    tray.Execute(1)
    tray.Finish()

    del tray

def run_gcd_audit(path, logger):
    from I3Tray import *

    try:
        tray = I3Tray()
        tray.AddModule("I3Reader","readGCD", filename = path)
        tray.AddModule('I3GCDAuditor', 'GCDAuditor', MaximumParanoia=True)

        tray.Execute()
        tray.Finish()

        del tray

        return 0
    except Exception, err:
        del tray
        logger.error("AuditGCD Error: " + str(err))
        return 1

def parse_gcd_audit_output(path, logger):
    """
    Parses the gcd audit and returns `True` if everything is OK, otherwise `False`.

    Args:
        path (str): Path of the logfile
        logger (Logger): The logger of the script

    Returns:
        boolean: `True` if everything is OK, otherwise `False.
    """

    with open(path, 'r') as f:
        l = str(f.read())
        l = l.split("\n")

        logger.info("====Start GCDAudit Log===")
        for line in l:
            logger.info(line)
        logger.info("===End GCDAudit Log====")

        h = [n for n in l if("GCDAuditor" in n \
                          and "ERROR" in n \
                          and "OMOMKey(19,60,0)" not in n)
                        ]

        if not len(h):   
            logger.info("GCD Audit for OK")
            return True
        else:
            logger.error("GCD audit failed)
            logger.info("====== GCD output after filtering ====")
            for line in h.split('\n'):
                logger.info(line)
            logger.info("====== End GCD output after filtering ====")
            return False

def parse_bad_dom_audit(path, logger):
    with open(path,'r') as f:
        d = str(f.read())
        d = d.split("\n")

        logger.info("====Start BadDoms Log===")
        for line in d:
            logger.info(line)
        logger.info("===End BadDoms Log====")

        b = [n for n in d if "BadDOMAuditor" in n \
                    and "ERROR" in n \
                    and "OMOMKey" in n]

        if not len(b):
            return True

        else:
            logger.error("BadDOMs audit failed")
            logger.info("====== Bad dom audit output after filtering ====")
            for line in b.split('\n'):
                logger.info(line)
            logger.info("====== End bad dom audit output after filtering ====")
            return False

def rehydrate(gcd_path, input_path, tmp_name, logger):
    from I3Tray import *
    tray = I3Tray()

    tray.AddModule( "I3Reader", "Reader", Filenamelist = [gcd_path, input_path])
    tray.AddSegment(Rehydration, 'rehydrator', doNotQify = False)
    tray.AddModule('I3Writer', 'OutWriter', FileName = tmp_name, Streams = [icetray.I3Frame.DAQ, icetray.I3Frame.Physics])
    tray.AddModule("TrashCan","trash")
    tray.Execute()
    tray.Finish()

    del tray

def run_bad_dom_audit(gcd_path, rehydrated_input_file, logger):
    from I3Tray import *
    from config import get_config()
    tray = I3Tray()
    tray.AddModule('I3Reader', 'reader', FilenameList = [gcd_path, rehydrated_input_file])
    tray.AddModule('I3BadDOMAuditor', 'BadDOMAuditor',
                    BadDOMList = get_config(logger).get('GCD', 'BadDomListNameSLC'),
                    Pulses = ['InIcePulses', 'IceTopPulses'],
                    IgnoreOMs = [OMKey(12, 65), OMKey(12, 66), OMKey(62, 65), OMKey(62, 66)],
                    UseGoodRunTimes = True)
    tray.AddModule('TrashCan', 'can')

    tray.Execute()
    tray.Finish()

    del tray


