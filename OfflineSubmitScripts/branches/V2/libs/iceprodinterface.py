
from abc import ABCMeta, abstractmethod

class IceProdInterface(object):
    __metaclass__ = ABCMeta

    def __init__(self, logger, dryrun):
        self.logger = logger
        self.dryrun = dryrun

    @abstractmethod
    def submit_run(self, dataset_id, run, checksumcache, source_file_type, gcd_file = None, special_files = [], aggregate = 1):
        pass

    @abstractmethod
    def clean_run(self, dataset_id, run):
        pass

    @abstractmethod
    def get_run_status(self, dataset_id, run):
        """
        Checks the run status.

        Args:
            dataset_id (int): The dataset_id
            run (runs.Run): The run

        Returns:
            str: Returns the status as str-code: OK, NOT_SUBMITTED, PROCESSING, ERROR. Error can still mean PROCESSING but with errors
        """
        pass

    @abstractmethod
    def is_run_submitted(self, dataset_id, run):
        pass

    @abstractmethod
    def add_file_to_catalog(self, dataset_id, run, path):
        """
        Args:
            path (files.File): The file
        """
        pass

    @abstractmethod
    def remove_file_from_catalog(self, dataset_id, run, path):
        """
        Args:
            path (files.File): The file
        """
        pass

    @abstractmethod
    def update_file_in_catalog(self, dataset_id, run, path):
        """
        Args:
            path (files.File): The file
        """
        pass

    @abstractmethod
    def get_jobs(self, dataset_id, run):
        """
        Returns informations about all jobs for a specific dataset/run combination.

        Args:
            dataset_id (int): The dataset id
            run (runs.Run): The run

        Returns:
            dict: Information about all jobs. The dict looks like: `{<sub_rub_id>: {'job_id': <>, 'input': [{'path': <>, 'md5': <>, 'sha512': <>}, ...], 'output': [{'path': <>, 'md5': <>, 'sha512': <>}, ...]}, ...}`
        """
        pass
