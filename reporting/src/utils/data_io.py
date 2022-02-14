import os
import time

from pyspark.sql import DataFrame, SparkSession


class DataIO:
    """Class for data input-output operations - Read and Write to Sources and Sinks"""

    @staticmethod
    def read_csv_from_disk(path: str, filename: str, spark: SparkSession):
        """Utility method to read csv from disk
       :param path: path to input file
       :param filename: name of file
       :param spark: SparkSession object
       :raises ValueError if file or path is invalid
        """
        try:
            path_to_csv = os.path.join(path + filename)
            return spark.read.option("header", True).option('delimiter', ',').format('csv').load(path_to_csv)
        except Exception as e:
            raise Exception("Invalid path or file not found")

    @staticmethod
    def write_df_to_jdbc(connection: dict, df: DataFrame):
        """Utility method to write dataframe through jdbc (postgres)
        :param connection: dictionary of connection params
        :param df: input dataframe to insert
        :raises ValueError if file or path is invalid
        """
        try:
            df.write \
                .jdbc(url=connection['url'],
                      table='covid_master_data',
                      mode='overwrite',
                      properties=connection)
            time.sleep(5)
            print("\nData has been written to postgres table.....")
        except ConnectionError as e:
            raise ConnectionError(e)

    @staticmethod
    def read_jdbc_to_df(connection: dict, spark: SparkSession):
        """Utility method to read a table through jdbc to spark dataframe
        :param connection: dictionary of connection params
        :param spark: SparkSession object
        :raises ConnectionError if connection error occurs
        """
        try:
            return spark.read.jdbc(url=connection['url'],
                                   table="covid_master_data",
                                   properties=connection)
        except ConnectionError as e:
            raise ConnectionError(e)
