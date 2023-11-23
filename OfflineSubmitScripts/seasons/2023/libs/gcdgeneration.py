
from icecube import icetray, dataclasses
from icecube.icetray import I3Tray

#from path import get_rootdir

from icecube.gcdserver.GCDGeneratorModule import GCDGenerator

from icecube.filterscripts_v2.icetop_GCDmodification.overwrite_snowheights import ChangeSnowHeights_FromDB

def get_latest_transaction_of_gcd_db(logger):
    from libs.config import get_config
    import pymongo
    from icecube.gcdserver.OptionParser import DEFAULT_DB_PASS, DEFAULT_DB_USER

    client = pymongo.MongoClient(get_config(logger).get('GCDGeneration', 'MongoDBHost'))

    try:
        client.omdb.collection_names()
        # We have all privileges on db
    except:
        # We need to authenticate
        client.omdb.authenticate(DEFAULT_DB_USER, DEFAULT_DB_PASS)

    collection = client.omdb.transaction

    transactions = collection.find().sort('transaction', pymongo.DESCENDING)

    if not transactions.count():
        logger.critical('No transaction found')
        raise Exception('No transaction found')

    return transactions[0]

def set_production_information(frame, production_version, snapshot_id, good_start_time, good_stop_time):
    frame["OfflineProductionVersion"] = dataclasses.I3Double(production_version)
    frame["GRLSnapshotId"] = dataclasses.I3Double(snapshot_id)
    frame["GoodRunStartTime"] = good_start_time
    frame["GoodRunEndTime"] = good_stop_time

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
        logger.warning("Replaced with End Time from i3live: {0}".format(run.get_stop_time()))
        frame['I3DetectorStatus'].end_time = run.get_stop_time()

def generate_gcd(run, gcd_path, spe_correction_file, logger):
    from icecube.BadDomList.BadDomListTraySegment import BadDomList
    from icecube.phys_services.spe_fit_injector import I3SPEFitInjector
    from icecube import dataio
    from .config import get_config

    tray = I3Tray()

    tray.Add(GCDGenerator, "GCDGenerator",
             RunId = run.run_id,
             I3LiveHost = get_config(logger).get('GCDGeneration', 'I3LiveHost'),
             MongoDBHost = get_config(logger).get('GCDGeneration', 'MongoDBHost'))
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
    tray.Add(ChangeSnowHeights_FromDB, 'updateSnowHeights', Run = run.run_id)   # <---------- NEW THING
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
    try:
        tray = I3Tray()
        tray.AddModule("I3Reader","readGCD", filename = path)
        tray.AddModule('I3GCDAuditor', 'GCDAuditor', MaximumParanoia=True)
        tray.Execute()
        tray.Finish()

        del tray

        return 0
    except Exception as err:
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
            logger.error("GCD audit failed")
            logger.info("====== GCD output after filtering ====")
            for line in h:
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
            for line in b:
                logger.info(line)
            logger.info("====== End bad dom audit output after filtering ====")
            return False

def rehydrate(gcd_path, input_path, tmp_name, logger):
    from icecube.filterscripts.offlineL2.Rehydration import Rehydration

    tray = I3Tray()

    tray.AddModule( "I3Reader", "Reader", Filenamelist = [gcd_path, input_path])
    tray.AddSegment(Rehydration, 'rehydrator', doNotQify = False)
    tray.AddModule('I3Writer', 'OutWriter', FileName = tmp_name, Streams = [icetray.I3Frame.DAQ, icetray.I3Frame.Physics])
    tray.AddModule("TrashCan","trash")
    tray.Execute()
    tray.Finish()

    del tray

def run_bad_dom_audit(gcd_path, rehydrated_input_file, logger):
    from .config import get_config

    logger.debug('BadDOMList = {0}'.format(get_config(logger).get('GCD', 'BadDomListNameSLC')))

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


