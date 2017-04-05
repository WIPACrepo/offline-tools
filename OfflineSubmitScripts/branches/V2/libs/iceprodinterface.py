
from abc import ABCMeta, abstractmethod

class IceProdInterface(object):
    __metaclass__ = ABCMeta

    def __init__(self, logger, dryrun):
        self.logger = logger
        self.dryrun = dryrun

    @abstractmethod
    def submit_run(self, dataset_id, run, checksumcache):
        pass

    @abstractmethod
    def clean_run(self, dataset_id, run):
        pass

    @abstractmethod
    def get_run_status(self):
        pass

    @abstractmethod
    def is_run_submitted(self, dataset_id, run):
        pass

