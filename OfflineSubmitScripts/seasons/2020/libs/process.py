#!/usr/bin/env python

"""
Helpers to manage processes.
"""

import subprocess as sub
import os
from . import files
import datetime

from libs.path import get_tmpdir

class Lock:
    def __init__(self, script_name, logger, lock_file = None):
        """
        Args:
            script_name (str): The main script name. Usually it's sufficient if you pass
                               `os.path.basename(__file__)`
            logger (logging.logger): The logger
            lock_file (str): Path to the lock file. Default is `None`, so, it builts its own path (`tmp` dir and `script_name.lock`).
        """

        self._script_name = script_name

        if lock_file is None:
            # Create lock file path: $TMP_DIR_PATH/<script_name>.lock
            # From <script_name> is the file extension removed
            self._lock_file = os.path.join(get_tmpdir(), os.path.splitext(os.path.basename(script_name))[0] + '.lock')
        else:
            self._lock_file = lock_file

        self._lock_file_owned_by_other = False
        self._logger = logger

    def lock(self, exit_if_running = True):
        """
        Trys to lock this process. If another instance of this script is running, it calls `exit(0).

        It utilizes a lock file that holds the PID.
        If the lock file exists, it checks if a process with the PID is still running.
        If such a process is running, it checks if the command contains the `script_name`.
        
        Args:
            exit_if_running (bool): If it's set to `True`, it will call `exit(0)` if the process is still (or another instance) is running. Default is `True`.
                                    If you pass `False`, see section of what this method returns.

        Returns:
            bool: returns `True` if the process is running. Otherwise, it returns `False`.
        """

        # Check if lock file exists. If it does, there might be a process still running
        if os.path.isfile(self._lock_file):
            self._logger.debug("Lock file %s exists"%self._lock_file)

            f = open(self._lock_file ,'r')
            pid = f.readline()

            self._logger.debug("Found PID %s in lock file"%str(pid))

            # Check if a process with this pid is still running, just printing the command w/o the ps header (so, no line if no process with PID is running)
            sub_proc = sub.Popen(['ps', '-p', str(pid), '-o', 'command='], shell=False, stdout=sub.PIPE)
            for line in sub_proc.stdout:
                # Check if the running process is still a PoleGCDCheck (is required since the PIDs are recycled)

                self._logger.debug("Check line: %s"%line)

                if self._script_name in line:
                    self._logger.debug("Line contains script name: %s"%self._script_name)
                    self._logger.info("Another instance of %s is running @ %s"%(self._script_name, datetime.datetime.now().isoformat().replace("T"," ")))
                    if exit_if_running:
                        self._logger.info("Exiting...")
                        self._lock_file_owned_by_other = True
                        exit(0)
                    else:
                        return True
        
            self._logger.info("Removing stale lock file")
            os.remove(self._lock_file)
        else:
            self._logger.debug("Lock file %s does not exist"%self._lock_file)
        
        # Ok, it's not running. Lets store the current PID and proceed
        with open(self._lock_file ,'w') as f:
            pid = str(os.getpid())

            self._logger.debug("Write PID (%s) to lock file: %s"%(pid, self._lock_file))
            f.write(pid)
        

        return False

    def unlock(self):
        """
        Removes the lock file. Should be called at least at the end of the script.

        Returns:
            bool: Returns `True` if the lock file has been removed. Returns `False` if no lock file exists.
        """
        self._logger.debug("Attempt to remove lock file")
        
        if self._lock_file_owned_by_other:
            self._logger.debug("Lock file has not been removed since it is probably owned by another running process")
            return False

        if os.path.isfile(self._lock_file):
            self._logger.info("Removing lock file")
            os.remove(self._lock_file)

            return True
        else:
            self._logger.debug("Lock file does not exist: %s"%self._lock_file)
            return False

    def __del__(self):
        """
        Checks if the lock file has been removed. It calls unlock() and logs a warning if the files has been removed.
        """
        self._logger.debug("Execute destructor")
        if self.unlock():
            self._logger.warning("Lock file has been removed in destructor. Please call unlock() at the end of your script to do so.")

