import configparser
import os

from pyspark.sql import SparkSession

from reporting.src.data_analysis import DataAnalysis
from reporting.src.utils.data_io import DataIO

if __name__ == "__main__":

    # Create a Spark application and return spark session
    spark = (
        SparkSession.builder.appName("Covid data analysis - PySpark")
            .config("spark.jars","/opt/spark/jars/postgresql-42.2.5.jar")
            .getOrCreate()
    )
    config = configparser.ConfigParser()
    config.read("db_properties.ini")

    db_args = config['postgres']
    db_args['password'] = os.environ['PG_PWD']

    # read csv from disk
    path = "/opt/covid/"
    input_df = DataIO.read_csv_from_disk(path, "owid-covid-data.csv", spark)

    # write to postgres table
    DataIO.write_df_to_jdbc(db_args, input_df)

    # read data from table
    table_df = DataIO.read_jdbc_to_df(db_args, spark)

    # transform dataframe and show reports on console
    DataAnalysis(table_df).check_sanity()
    DataAnalysis(table_df).get_top_5_vaccine_countries().show()
    DataAnalysis(table_df).get_weekly_trend_new_cases().show()

