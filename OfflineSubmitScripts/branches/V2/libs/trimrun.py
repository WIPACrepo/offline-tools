
import os

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
    or if the start/stop time is within a file, the file will be trimmed.

    Args:
        run (Run): The run
        logger (Logger): The logger
        dryrun (boolean): If `True`, no data will be deleted
    """

    from config import get_config

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

            iceprod.remove_file_from_catalog(dataset_id, run, f)

    # Check if a file needs to be trimmed
    if good_l2_files[0].get_start_time() < run.get_good_start_time():
        logger.info('First subrun in good time range needs to be trimmed: sub_run_id = {sub_run_id}, start_time = {start_time}, stop_time = {stop_time}, good_start_time = {good_start_time}, good_stop_time = {good_stop_time}'.format(
            sub_run_id = good_l2_files[0].sub_run_id,
            start_time = good_l2_files[0].get_start_time(),
            stop_time = good_l2_files[0].get_stop_time(),
            good_start_time = run.get_good_start_time(),
            good_stop_time = run.get_good_stop_time()
        ))
        trim_sub_run(iceprod, dataset_id, good_l2_files[0], logger, dryrun)

    if good_l2_files[-1].get_stop_time() > run.get_stop_time():
        logger.info('Last subrun in good time range needs to be trimmed: sub_run_id = {sub_run_id}, start_time = {start_time}, stop_time = {stop_time}, good_start_time = {good_start_time}, good_stop_time = {good_stop_time}'.format(
            sub_run_id = good_l2_files[-1].sub_run_id,
            start_time = good_l2_files[-1].get_start_time(),
            stop_time = good_l2_files[-1].get_stop_time(),
            good_start_time = run.get_good_start_time(),
            good_stop_time = run.get_good_stop_time()
        ))
        trim_sub_run(iceprod, dataset_id, good_l2_files[-1], logger, dryrun)

def trim_sub_run(iceprod, dataset_id, sub_run, logger, dryrun)
    from path import get_tmpdir
    from iceube import dataio, dataclasses, icetray
    from I3Tray import *

    # bzip2 -f of a zero-sized file results in a 
    # 14 byte large file 
    if sub_run.size() <= 14:
        logger.error('Cannot trim {0} because the file size is <= 14 byte (bzip2 -f of an empty file results in 14-byte-sized file)'.format(sr.path))
        return

    tmp_file = os.path.join(get_tmpdir(), 'Trimmed_' + sub_run.get_name())

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

    logger.info("Moving {0} to {1}".format(tmp_file, sub_run.path))
    if not dryrun:
        os.rename(tmp_file, sub_run.path)

    iceprod.update_file_in_catalog(dataset_id, sub_run.run, sub_run)

    # TODO
    # re-write gaps.txt file using new (trimmed) .i3 file
    if InFile_ == InFile:
        TFile = dataio.I3File(InFile_)
        FirstEvent = TFile.pop_frame()['I3EventHeader']
        while TFile.more(): LastEvent = TFile.pop_frame()['I3EventHeader']
        TFile.close()
        GFile = InFile.replace(".i3.bz2","_gaps.txt")
        if os.path.isfile(GFile):
            UpdatedGFile = os.path.join(os.path.dirname(GFile),'Updated_'+os.path.basename(GFile))
            # for dryrun, write the stuff to a temporary file
            if dryrun:
                UpdatedGFile = os.path.join(get_tmpdir(),os.path.split(UpdatedGFile)[1])
                logger.info("--dryrun set, writing to temporary file %s" %UpdatedGFile)
            with open(UpdatedGFile,"w") as u:
                u.write('Run: %s\n'%FirstEvent.run_id)
                u.write('First Event of File: %s %s %s\n'%(FirstEvent.event_id,\
                                                         FirstEvent.start_time.utc_year,\
                                                         FirstEvent.start_time.utc_daq_time)
                        )
                u.write('Last Event of File: %s %s %s\n'%(LastEvent.event_id,\
                                                         LastEvent.end_time.utc_year,\
                                                         LastEvent.end_time.utc_daq_time)
                        )
                u.write("File Livetime: %s\n"%str((LastEvent.end_time - FirstEvent.start_time)/1e9))
                    
            logger.info("moving %s to %s"%(UpdatedGFile,GFile))
            if not dryrun:
                os.system("mv -f %s %s"%(UpdatedGFile,GFile))
                dbs4_.execute("""update i3filter.urlpath u
                            set md5sum="%s", size="%s"
                            where u.dataset_id=%s 
                            and concat(substring(u.path,6),"/",u.name) = "%s" """%\
                            (str(FileTools(GFile, logger).md5sum()),str(os.path.getsize(GFile)), dataset_id, GFile))
            else: # here we actually have to do something
                  # as we haven't overwritten the original file
                  # we have a stale Trimmed_ file...
                os.remove(TrimmedFile)

