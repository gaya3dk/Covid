from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_sub, current_date, max, lit


class DataQualityCheck:
    """ Class to implement data quality checks on input spark dataframe, checks are not exhaustive """

    @staticmethod
    def expect_non_empty_data(df: DataFrame):
        """
        Utility method to check for empty dataset
       :param df: input dataframe
       :raises ValueError
       """
        if df.count() == 0:
            raise ValueError(f"Non-empty dataset expected")

    @staticmethod
    def expect_date_column(df: DataFrame):
        """
        Utility method to check for presence of date column
       :param df: input dataframe
       :raises ValueError
       """
        if 'date' not in df.columns:
            raise Exception(f"Dataset doesn't have date column")

    @staticmethod
    def expect_fresh_data(df: DataFrame, freshness_days=2):
        """
         Utility method to check freshness of dataset, default is 2
        :param df: input dataframe
        :param freshness_days: number of days delay in data freshness, default is 2
        :raises ValueError
        """
        df = df.filter(col('date') >= date_sub(current_date(), freshness_days))
        if df.count() == 0:
            raise ValueError(f"Dataset does not meet expected freshness of {freshness_days} day(s)")

    @staticmethod
    def expect_valid_numeric_data(df: DataFrame):
        """
         Utility method to check if key numeric fields contain only positive values from latest date
        :param df: input dataframe
        :raises ValueError
        """
        max_date = df.agg(max(df.date)).collect()[0][0]
        df = df \
            .filter(df.date == lit(max_date)) \
            .filter((col('total_cases') < 0) | (col('new_cases') < 0) | (col('population') < 0))
        if df.count() != 0:
            raise ValueError(f"Number of cases or New cases or Population cannot be negative")
