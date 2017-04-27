
import sys
from libs.config import get_config

sys.path.append(get_config().get('DEFAULT', 'ProductionToolsPath'))
from FileTools import *

class ChecksumCache(object):
    def __init__(self, logger, ctypes = ['md5', 'sha512']):
        self.logger = logger
        self._data = {ctype: {} for ctype in ctypes}

    def has_checksum(self, path, ctype):
        """
        Checks if the cache has the checksum for the given path and type.

        Args:
            path (str): The path to the file
            ctype (str): Type of checksum

        Returns:
            boolean: `True` if the checksum for the given path is in the cache.
        """

        return path in self._data[ctype]

    def has_md5(self, path):
        """
        Checks if the cache has the checksum for the given path.

        Args:
            path (str): The path to the file

        Returns:
            boolean: `True` if the checksum for the given path is in the cache.
        """

        return self.has_checksum(path, 'md5')

    def has_sha512(self, path):
        """
        Checks if the cache has the checksum for the given path.

        Args:
            path (str): The path to the file

        Returns:
            boolean: `True` if the checksum for the given path is in the cache.
        """

        return self.has_checksum(path, 'sha512')

    def get_checksum(self, path, ctype):
        """
        Returns the checksum of the path for the given type. If the path is not found in the cache, it will be calculated, added to the cache and returned.

        Args:
            path (str): Path to the file
            ctype (str): Type of checksum

        Returns:
            str: The checksum.
        """

        if self.has_checksum(path, ctype):
            return self._data[ctype][path]
        else:
            self.logger.warning('Checksum for {0} not in cache. Calculating checksum...'.format(path))
            return self.set_checksum(path, ctype)

    def get_md5(self, path):
        """
        Returns the MD5 sum of the path. If the path is not found in the cache, it will be calculated, added to the cache and returned.

        Args:
            path (str): Path to the file

        Returns:
            str: MD5 checksum.
        """

        return self.get_checksum(path, 'md5')

    def get_sha512(self, path):
        """
        Returns the SHA512 sum of the path. If the path is not found in the cache, it will be calculated, added to the cache and returned.

        Args:
            path (str): Path to the file

        Returns:
            str: SHA512 checksum.
        """

        return self.get_checksum(path, 'sha512')

    def set_checksum(self, path, ctype, checksum = None):
        """
        Sets the checksum of the given path for the given type.

        Args:
            path (str): Path to the file
            ctype (str): Type of checksum
            checksum (str): The checksum of the file. If it is `None` (as default) the checksum will be calculated and then added.

        Returns:
            str: The checksum
        """
        self._data[ctype][path] = checksum or FileTools(path, logger = self.logger).checksum(ctype)
        return self._data[ctype][path]

    def set_md5(self, path, checksum = None):
        """
        Sets the MD5 checksum of the given path.

        Args:
            path (str): Path to the file
            checksum (str): The checksum of the file. If it is `None` (as default) the checksum will be calculated and then added.

        Returns:
            str: The checksum
        """

        return self.set_checksum(path, 'md5', checksum)

    def set_sha512(self, path, checksum = None):
        """
        Sets the SHA512 checksum of the given path.

        Args:
            path (str): Path to the file
            checksum (str): The checksum of the file. If it is `None` (as default) the checksum will be calculated and then added.

        Returns:
            str: The checksum
        """

        return self.set_checksum(path, 'sha512', checksum)

class DBChecksumCache(ChecksumCache):
    """
    This Cache uses a MySQL DB as cache. All queried checksums are held in a dict.
    """

    def __init__(self, logger, dryrun):
        from databaseconnection import DatabaseConnection
        self.db = DatabaseConnection.get_connection('filter-db', logger)

        ChecksumCache.__init__(self, logger, ['md5'])

        self.dryrun = dryrun

    def has_checksum(self, path, ctype):
        if ctype != 'md5':
            raise Exception('Only MD5 checksum are supported yet')

        if not ChecksumCache.has_checksum(self, path, ctype):
            sql = """
                SELECT `md5`
                FROM i3filter.checksum_cache
                WHERE path = '{path}'
            """.format(path = path)

            self.logger.debug('SQL: {0}'.format(sql))

            query = self.db.fetchall(sql)
            if len(query) == 1:
                self._data[ctype][path] = query[0]['md5']
            elif len(query) == 0:
                return False
            else:
                raise Exception('This case should actually never happen...')

        return True

    def set_checksum(self, path, ctype, checksum = None):
        ChecksumCache.set_checksum(self, path, ctype, checksum)

        if ctype != 'md5':
            raise Exception('Only MD5 checksum are supported yet')

        sql = """
            INSERT INTO i3filter.checksum_cache (`path`, `md5`)
            VALUES ('{path}', '{md5}')
            ON DUPLICATE KEY UPDATE `md5` = '{md5}'
        """.format(path = path, md5 = self._data[ctype][path])

        self.logger.debug('SQL: {0}'.format(sql))

        if not self.dryrun:
            self.db.execute(sql)

    def get_checksum(self, path, ctype):
        if self.has_checksum(path, ctype):
            return self._data[ctype][path]
        else:
            self.logger.warning('Checksum for {0} not in cache. Calculating checksum...'.format(path))
            return self.set_checksum(path, ctype)

class Counter(object):
    def __init__(self, names):
        """
        Args:
            names (list): List of counter names, e.g. ['validated', 'skipped', 'error']
        """

        self.counter = {n: 0 for n in names}

    def count(self, name):
        self.counter[name] += 1

    def get(self, name):
        return self.counter[name]

    def get_summary(self):
        return ' | '.join(['{name}: {value}'.format(name = name, value = value) for name, value in self.counter.items()])

    def print_summary(self, prefix = ''):
        print prefix + self.get_summary()

