
import os

from icecube import dataio, dataclasses, icetray
from I3Tray import *

class TrimFileClass(icetray.I3PacketModule):
    """
    Can be given start and end times considered to be "good"
    - e.g. from the grl table - and will truncate events
    which are outside this range
    """

    def __init__(self, context):
        icetray.I3PacketModule.__init__(self, context, icetray.I3Frame.DAQ)
        self.framecount = 0
        self.AddParameter('GoodStart',
                           'The good start time of the run',
                           None)

        self.AddParameter('GoodEnd',
                          'The good end time of the run',
                           None)
        self.AddParameter('logger',
                          'A logging.logger instance',
                           None)

        self.AddOutBox('OutBox')

    def Finish(self):
        self.logger.debug("Should have written {0} frames".format(self.framecount))
        icetray.I3PacketModule.Finish(self)

    def Configure(self):
        self.GoodStart = self.GetParameter("GoodStart")
        self.GoodEnd = self.GetParameter("GoodEnd")
        self.logger = self.GetParameter("logger")

        assert self.GoodStart is not None
        assert self.GoodEnd is not None

    def FramePacket(self, frames):
        if frames[0]['I3EventHeader'].start_time >= self.GoodStart and \
                frames[0]['I3EventHeader'].end_time >= self.GoodStart and \
                frames[0]['I3EventHeader'].start_time <= self.GoodEnd and \
                frames[0]['I3EventHeader'].end_time <= self.GoodEnd and\
                icetray.I3Int(len(frames) - 1) > 0 : # only needed to take care of previously mis-trimmed files  with "barren" Q frames
                    for fr in frames:
                        self.framecount += 1
                        self.PushFrame(fr)
        else:
            # this frames will be thrown away
            self.logger.debug("Throwing away frame not in good run range!")
            self.logger.debug("GoodStart: {0}".format(self.GoodStart.__repr__()))
            self.logger.debug("GoodEnd: {0}".format(self.GoodEnd.__repr__()))
            self.logger.debug("Frame start time: {0}".format(frames[0]["I3EventHeader"].start_time.__repr__()))

def trim_to_good_run_time_range(iceprod, dataset_id, run, logger, dryrun):
    """
    Checks if the L2 output files of the given run are within the good start/stop times.
    If not, the run start/stop files that are outside the range will be moved to a sub folder
    or if the start/stop time is within a file, the file will be trimmed (but the original file will be kept
    in the 'bad do not use' folder).

    Args:
        run (Run): The run
        logger (Logger): The logger
        dryrun (boolean): If `True`, no data will be deleted
    """

    from config import get_config
    from runs import SubRun

    bad_sub_run_folder = run.format(get_config(logger).get('Level2', 'BadSubRunFolder'))

    l2files = SubRun.sort_sub_runs(run.get_level2_files())
    good_l2_files = []
    bad_l2_files = []

    for f in l2files:
        if f.is_in_good_time_range():
            good_l2_files.append(f)
        else:
            bad_l2_files.append(f)

    if len(bad_l2_files):
        if not os.path.isdir(bad_sub_run_folder):
            logger.debug('Create bad sub run folder: {0}'.format(bad_sub_run_folder))
            if not dryrun:
                os.mkdir(bad_sub_run_folder)

        for f in bad_l2_files:
            dest = os.path.join(bad_sub_run_folder, f.get_name())

            logger.info('Move {0} to bad {1} since this sub run is not in good time range'.format(f.path, dest))
            if not dryrun:
                os.rename(f.path, dest)

            logger.debug('Remove file from catalog')
            iceprod.remove_file_from_catalog(dataset_id, run, f)

            logger.info(f.format('Mark sub run {run_id}/{sub_rub_id} as bad'))
            # Note: Dryrun is considered within this method:
            f.mark_as_bad()

    # Check if a file needs to be trimmed
    if good_l2_files[0].get_start_time() < run.get_good_start_time():
        logger.info('First subrun in good time range needs to be trimmed: sub_run_id = {sub_run_id}, start_time = {start_time}, stop_time = {stop_time}, good_start_time = {good_start_time}, good_stop_time = {good_stop_time}'.format(
            sub_run_id = good_l2_files[0].sub_run_id,
            start_time = good_l2_files[0].get_start_time(),
            stop_time = good_l2_files[0].get_stop_time(),
            good_start_time = run.get_good_start_time(),
            good_stop_time = run.get_good_stop_time()
        ))

        if not os.path.isdir(bad_sub_run_folder):
            logger.debug('Create bad sub run folder: {0}'.format(bad_sub_run_folder))
            if not dryrun:
                os.mkdir(bad_sub_run_folder)

        trim_sub_run(iceprod, dataset_id, good_l2_files[0], bad_sub_run_folder, logger, dryrun)

    if good_l2_files[-1].get_stop_time() > run.get_stop_time():
        logger.info('Last subrun in good time range needs to be trimmed: sub_run_id = {sub_run_id}, start_time = {start_time}, stop_time = {stop_time}, good_start_time = {good_start_time}, good_stop_time = {good_stop_time}'.format(
            sub_run_id = good_l2_files[-1].sub_run_id,
            start_time = good_l2_files[-1].get_start_time(),
            stop_time = good_l2_files[-1].get_stop_time(),
            good_start_time = run.get_good_start_time(),
            good_stop_time = run.get_good_stop_time()
        ))

        if not os.path.isdir(bad_sub_run_folder):
            logger.debug('Create bad sub run folder: {0}'.format(bad_sub_run_folder))
            if not dryrun:
                os.mkdir(bad_sub_run_folder)

        trim_sub_run(iceprod, dataset_id, good_l2_files[-1], bad_sub_run_folder, logger, dryrun)

def trim_sub_run(iceprod, dataset_id, sub_run, bad_sub_run_folder, logger, dryrun):
    from path import get_tmpdir
    from files import GapsFile

    # bzip2 -f of a zero-sized file results in a 
    # 14 byte large file 
    if sub_run.size() <= 14:
        logger.error('Cannot trim {0} because the file size is <= 14 byte (bzip2 -f of an empty file results in 14-byte-sized file)'.format(sr.path))
        return

    tmp_file = os.path.join(get_tmpdir(), 'Trimmed_' + sub_run.get_name())
    tmp_gaps_file = os.path.join(get_tmpdir(), 'Trimmed_' + sub_run.get_gaps_file().get_name())

    # The actual trimming is done with the TrimFileClass,
    # so it is required to boot I3Tray
    tray = I3Tray()
    tray.AddModule("I3Reader","readL2File", filename = sub_run.path)
    tray.AddModule(TrimFileClass, 'Trim',
                   GoodStart = sub_run.run.get_good_start_time(),
                   GoodEnd = sub_run.run.get_good_stop_time(),
                   logger = logger)
    tray.AddModule('I3Writer', 'FileWriter',
                    FileName = tmp_file,
                    Streams = [icetray.I3Frame.DAQ,
                               icetray.I3Frame.Physics]
    )
    tray.AddModule("TrashCan","trash")
    tray.Execute()
    tray.Finish()

    # Move files
    new_subrun_name = os.path.join(bad_sub_run_folder, sub_run.get_name())

    logger.info("Moving {0} to {1}".format(sub_run.path, new_subrun_name))
    logger.info("Moving {0} to {1}".format(tmp_file, sub_run.path))
    if not dryrun:
        os.rename(sub_run.path, new_subrun_name)
        os.rename(tmp_file, sub_run.path)

    iceprod.update_file_in_catalog(dataset_id, sub_run.run, sub_run)

    # re-write gaps.txt file using new (trimmed) .i3 file
    logger.info('Re-creating the gaps file')
    gaps_file = GapsFile(tmp_gaps_file, logger)
    gaps_file.create_from_data(sub_run.path, overwrite = True) # Just overwriting the file in the tmp folder!

    if not dryrun:
        # OK, it's not a dryrun. Let's move the tmp gaps file to the correct place
        os.rename(tmp_gaps_file, sub_run.get_gaps_file().path)

        iceprod.update_file_in_catalog(dataset_id, sub_run.run, sub_run.get_gaps_file())

