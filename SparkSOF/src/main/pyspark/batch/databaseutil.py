import sys
import os
from pyspark.sql import DataFrameReader
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import config.config as config


class DatabaseOperations(object):

    def __init__(self):
        self.__db_name = config.JDBC_DB_NAME
        self.__db_user = config.JDBC_USER
        self.__db_pass = config.JDBC_PASSWORD
        self.__db_host = config.JDBC_HOST
        self.__db_port = config.JDBC_PORT

        self.__write_mode = 'append'
        self.__url = "jdbc:postgresql://" + \
                     self.__db_host + \
            ":" + \
                     self.__db_port + \
            "/" + \
                     self.__db_name

        self.__properties = {
            "driver": "org.postgresql.Driver",
            "user": self.__db_user,
            "password": self.__db_pass
        }

    # def set_table_name(self, table_name):
    #     '''
    #         Set table name
    #     '''
    #
    #     self.__table_name = table_name

    def db_write(self, dataframe, table_name, write_mode=None):
        '''
            This function takes in a dataframe and appends it to the table
        '''
        if write_mode is None:
            dataframe.write.jdbc(url=self.__url, table=table_name, mode=self.__write_mode, properties=self.__properties)
        else:
            dataframe.write.jdbc(url=self.__url, table=table_name, mode=write_mode,        properties=self.__properties)


    def db_read(self, table_name, sqlContext):
        '''
            This function takes in a table name and reads from a database
        '''
        dataframe = DataFrameReader(sqlContext).jdbc(
            url=self.__url, table=table_name, properties=self.__properties
        )
        return dataframe

