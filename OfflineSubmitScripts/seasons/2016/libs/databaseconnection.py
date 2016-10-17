
import pymysql

class DatabaseConnection:
    __db_connection = {}

    def __init__(self, user, password, host, database):
        self.__connection = pymysql.connect(user = user, password = password, host = host, database = database)

    def execute(self, sql):
        ret = None

        with self.__connection.cursor() as cursor:
            cursor.execute(sql)
            self.__connection.commit()
            ret = cursor

        return ret

    def fetchall(self, sql, UseDict = False):
        cursor_type = None

        if UseDict:
            cursor_type = pymysql.cursors.DictCursor

        ret = None

        with self.__connection.cursor(cursor_type) as cursor:
            cursor.execute(sql)
            ret = cursor.fetchall()

        return ret

    def close(self):
        self.__connection.close()

    @classmethod
    def get_connection(cls, name):
        if name != 'filter-db':
            return None

        if 'filter-db' not in cls.__db_connection.keys():
            cls.__db_connection['filter-db'] = cls(user = 'i3filter', password = '0a6f869d0c8fcc', host = 'filter-db.icecube.wisc.edu', database = 'i3filter')

        return cls.__db_connection[name]

