import os
import time

from pyspark.sql.utils import AnalysisException


class DataIO:
    "Class for data input-output operations - Read and Write to Sources and Sinks"

    @staticmethod
    def read_csv_from_disk(path, filename, spark):
        try:
            path_to_csv = os.path.join(path + filename)
            return spark.read.option("header", True).option('delimiter', ',').format('csv').load(path_to_csv)
        except Exception as e:
            raise Exception("Invalid path or file not found")

    @staticmethod
    def write_df_to_jdbc(connection, df):
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
    def read_jdbc_to_df(connection, spark):
        try:
            return spark.read.jdbc(url=connection['url'],
                                   table="covid_master_data",
                                   properties=connection)
        except ConnectionError as e:
            raise ConnectionError(e)
