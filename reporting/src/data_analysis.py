from datetime import datetime, timedelta

from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import max, col, weekofyear, year, lit, \
    concat, sum, lag, format_number

from reporting.src.utils.data_quality_check import DataQualityCheck


class DataAnalysis:

    def __init__(self, df):
        self.df = df

    def check_sanity(self):
        """
        Performs a set of quality checks on input raw data,
        Raises exception if any of the quality check is not met
       """
        print("#######################################################\n")
        print("Performing quality checks on the dataset...")
        DataQualityCheck.expect_non_empty_data(self.df)
        DataQualityCheck.expect_date_column(self.df)
        DataQualityCheck.expect_fresh_data(self.df)
        DataQualityCheck.expect_valid_numeric_data(self.df)
        print("Sanity checks complete, starting to generate reports...\n")
        print("#######################################################\n")

    def get_top_5_vaccine_countries(self):
        """
        Builds a tabular view of vaccination rate per country based on population
        and prints the top 5 countries
        :returns report_df : table containing top 5 vaccination countries with their vaccinate rates
        """
        valid_df = self.df \
            .filter(self.df.continent.isNotNull()) \
            .filter(self.df.location.isNotNull())

        # Get latest date
        max_date = self.df.agg(max(self.df.date)).collect()[0][0]
        print("###############################################################################\n")
        print(f"Top 5 countries as on {max_date} with the highest vaccination rate per population\n")
        print("###############################################################################\n")
        # group vaccinations per country
        df_by_country = valid_df \
            .where(valid_df.date == max_date) \
            .groupBy('continent', 'location') \
            .agg(max(valid_df.people_fully_vaccinated).alias("people_fully_vaccinated"),
                 max(valid_df.population).alias("population")) \
            .select('continent', 'location', 'people_fully_vaccinated', 'population')

        # calculate percentage of calculation as people_fully_vaccinated/population per country
        report_df = df_by_country \
            .withColumn("percentage_vaccination", (col('people_fully_vaccinated') / col('population') * 100)) \
            .select('continent', 'location', format_number('percentage_vaccination', 2).alias("percentage_vaccination")) \
            .orderBy(col('percentage_vaccination'), ascending=False).limit(5)

        return report_df

    def get_weekly_trend_new_cases(self):
        """
        Builds a tabular view of weekly trend - week on week % age difference of new cases
        for current and last 3 weeks
        :returns report_df : table containing weekly trend of new cases split by country
        """
        print("###################################################################\n")
        print("Week on Week trend of new cases from last 3 weeks by continent\n")
        print("###################################################################\n")

        # Filter out continent and country with null values
        valid_df = self.df\
            .filter(self.df.continent.isNotNull())\
            .filter(self.df.location.isNotNull())

        # pre-calculate columns containing week of year and year
        df_with_week = valid_df \
            .withColumn("week", weekofyear(valid_df.date)) \
            .withColumn("year", year(col('date'))) \
            .withColumn("week_of_year", concat(col('year'), lit("-W"), col('week')))

        # filter records from last 35 days - last 4 weeks + current week
        today = datetime.today().date()
        df_with_4weeks = df_with_week.filter(col('date') >= lit(today - timedelta(days=35)))

        # aggregate new cases per week
        new_cases = df_with_4weeks\
            .groupBy('week_of_year', 'week', 'continent')\
            .agg(sum('new_cases').cast('bigint').alias("new_cases"))

        # calculate week over week % increase or decrease in new cases using Window function
        result_df = DataAnalysis.calculate_lag(new_cases)

        # Build report per continent and weekly increase/decrease in new cases for current and last 3 weeks
        report_df = result_df\
            .groupBy("continent")\
            .pivot("week_of_year")\
            .agg(format_number(max('weekly_trend_percentage'), 2).alias("weekly_trend_percentage"))

        return report_df

    @staticmethod
    def calculate_lag(input_df: DataFrame):
        """
        Calculate change in new_cases column between weeks
        :param input_df: input spark dataframe to transform
        :returns output_df : table containing weekly trend calculated based on window function over every 2 consecutive weeks
        """
        df_lag = input_df \
            .withColumn('prev_week_cases', lag(input_df['new_cases'])
                        .over(Window.partitionBy("continent").orderBy('week')))
        output_df = df_lag \
            .filter(col('prev_week_cases').isNotNull())\
            .withColumn('weekly_trend_percentage',
                        ((df_lag['new_cases'] - df_lag['prev_week_cases']) / df_lag['prev_week_cases']) * 100)

        return output_df
