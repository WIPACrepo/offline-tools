"""
Tools which require to have a loaded env-shell
"""
from icecube import icetray
from logger import DummyLogger

class TrimFileClass(icetray.I3PacketModule):
    """
    Can be given start and end times considered to be "good"
    - e.g. from the grl table - and will truncate events
    which are outside this range
    """

    def __init__(self, context):
        icetray.I3PacketModule.__init__(self, context, icetray.I3Frame.DAQ)
        self.framecount = 0
        self.AddParameter('GoodStart',                     # name
                           'The good start time of the run',   # doc
                           None)                 # default

        self.AddParameter('GoodEnd',                 # name
                          'The good end time of the run',       # doc
                           None)      
        self.AddParameter('logger',                 # name
                          'A logging.logger instance',       # doc
                           DummyLogger())      
        self.AddParameter('dryrun',                 # name
                          'Don not work, just pretend',       # doc
                           False)      
        self.AddOutBox('OutBox')

    def Finish(self):
        self.logger.debug("Should have written %i frames" %self.framecount)
        icetray.I3PacketModule.Finish(self)

    def Configure(self):
        self.GoodStart = self.GetParameter("GoodStart")
        self.GoodEnd = self.GetParameter("GoodEnd")
        self.dryrun = self.GetParameter("dryrun")
        self.logger = self.GetParameter("logger")

        assert self.GoodStart is not None
        assert self.GoodEnd is not None

    def FramePacket(self, frames):
        if frames[0]['I3EventHeader'].start_time>=self.GoodStart and \
                frames[0]['I3EventHeader'].end_time>=self.GoodStart and \
                frames[0]['I3EventHeader'].start_time<=self.GoodEnd and \
                frames[0]['I3EventHeader'].end_time<=self.GoodEnd and\
                icetray.I3Int(len(frames) - 1)>0 : # only needed to take care of previously mis-trimmed files  with "barren" Q frames
                    for fr in frames:
                        self.framecount +=1
                        self.PushFrame(fr)

        else:
            # this frames will be thrown away
            self.logger.debug("Throwing away frame not in good run range!")
            self.logger.debug("GoodStart: %s" %self.GoodStart.__repr__())
            self.logger.debug("GoodEnd: %s" %self.GoodEnd.__repr__())
            self.logger.debug("Frame start time: %s"  %frames[0]["I3EventHeader"].start_time.__repr__())
            if self.dryrun:
                for fr in frames:
                    self.PushFrame(fr)

