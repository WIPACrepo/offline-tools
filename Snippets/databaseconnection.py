
import pymysql

class DatabaseConnection:
    __db_connection = {}

    def __init__(self, user, password, host, database, logger):
        self._user = user
        self._password = password
        self._host = host
        self._database = database
        self._logger = logger

        self._connect()

    def _connect(self):
        self.__connection = pymysql.connect(user = self._user, password = self._password, host = self._host, database = self._database)

    def execute(self, sql, reconnect = True):
        ret = None

        try:
            with self.__connection.cursor() as cursor:
                cursor.execute(sql)
                self.__connection.commit()
                ret = cursor
        except pymysql.Error as e:
            if e.args[0] == 2006 and reconnect:
                self._logger.warn('MySQL connection has been reset. Try to re-connect and do it again')

                self._connect()

                # Try this only once. If it fails again, stop it.
                self.execute(sql, reconnect = False)
            else:
                raise e

        return ret

    def fetchall(self, sql, UseDict = False, reconnect = True):
        cursor_type = None

        if UseDict:
            cursor_type = pymysql.cursors.DictCursor

        ret = None

        try:
            with self.__connection.cursor(cursor_type) as cursor:
                cursor.execute(sql)
                ret = cursor.fetchall()
        except pymysql.Error as e:
            if e.args[0] == 2006 and reconnect:
                self._logger.warn('MySQL connection has been reset. Try to re-connect and do it again')

                self._connect()

                # Try this only once. If it fails again, stop it.
                self.fetchall(sql, UseDict = UseDict, reconnect = False)
            else:
                raise e

        return ret

    def close(self):
        self.__connection.close()

    @classmethod
    def get_connection(cls, name, logger):
        installed_dbs = {
            'filter-db': {'user': 'i3filter_read', 'password': 'ce7f29816fd832', 'host': 'filter-db.icecube.wisc.edu', 'database': 'i3filter'},
            'sdst': {'user': 'sdst', 'password': 'Arch2Store', 'host': 'pilchard.icecube.wisc.edu', 'database': 'sdstprocessing'}
        }

        if name not in installed_dbs:
            return None

        if name not in cls.__db_connection.keys():
            cls.__db_connection[name] = cls(logger = logger, **installed_dbs[name])

        return cls.__db_connection[name]

