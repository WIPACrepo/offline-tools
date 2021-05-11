#!/usr/bin/env python
import subprocess

class SVN:
    # Share data for all instances. Avoids unnecessary executions of `svn` commands
    _data = {}

    def __init__(self, svn_path, logger = None, load_from_file = None):
        self._logger = logger
        self._svn_path = svn_path
        self._load_from_file = load_from_file

    def read_info(self, force = False):
        """
        Utilizes the `svn info` command in order to obtain the required information.

        Args:
            force (bool): If the info has been loaded once, it wont load it again except if `force` is set to `True`.
        """

        if self._svn_path in self._data.keys() and not force:
            return

        if self._load_from_file is not None:
            import json
            with open(self._load_from_file) as f:
                self._data = json.load(f)

            return

        self._data[self._svn_path] = {}

        info = subprocess.check_output(['svn', 'info', self._svn_path])
    
        for line in info.splitlines():
            if len(line.strip()) == 0:
                continue

            split = line.split(':', 1)

            if len(split) != 2:
                if self._logger is not None:
                    self._logger.warning("This line cannot be interpreted: '%s'" % line)

                continue

            if self._logger is not None:
                self._logger.debug("Line: '%s'" % line)

            key = split[0].strip()
            value = split[1].strip()

            if self._logger is not None:
                self._logger.debug("key = value: %s = %s" % (key, value))

            self._data[self._svn_path][key] = value

    def get(self, key):
        """
        Returns the value of the key.

        Loads the information on demand.

        Args:
            key (str): The key, e.g. `Revision`

        Returns:
            str: The value of the given key
        """

        self.read_info()

        return self._data[self._svn_path][key]

if __name__ == "__main__":
    from files import get_rootdir

    svn = SVN(svn_path = get_rootdir())

    print "Revision %s" % svn.get('Revision')
    print "Info: %s" % svn._data
