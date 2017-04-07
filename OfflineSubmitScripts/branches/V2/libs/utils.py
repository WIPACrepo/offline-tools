
class ChecksumCache(object):
    def __init__(self, logger, ctypes = ['md5', 'sha512']):
        from libs.config import get_config
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
        from files import File

        self._data[ctype][path] = checksum or File.get_checksum(path, ctype, self.logger)
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

        self.set_checksum(path, 'md5', checksum)

    def set_sha512(self, path, checksum = None):
        """
        Sets the SHA512 checksum of the given path.

        Args:
            path (str): Path to the file
            checksum (str): The checksum of the file. If it is `None` (as default) the checksum will be calculated and then added.

        Returns:
            str: The checksum
        """

        self.set_checksum(path, 'sha512', checksum)

