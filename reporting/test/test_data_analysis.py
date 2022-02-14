import os

import pytest

from reporting.src.data_analysis import DataAnalysis
from reporting.src.utils.data_io import DataIO
from pyspark.sql.functions import col

BASE_PATH = str(os.path.dirname(__file__)) + "/test_data/"


@pytest.fixture(scope="module")
def input_df(spark_session):
    """ Input dataframe to share in the session - valid data"""
    path = BASE_PATH
    filename = "sampledata4.csv"
    df = DataIO.read_csv_from_disk(path, filename, spark_session)
    return df


@pytest.mark.reporting
def test_get_top_5_vaccine_countries(input_df):
    """ Test if the resulting data is tabular and has 5 countries"""
    df = DataAnalysis(input_df).get_top_5_vaccine_countries()
    assert (df.select('location').count() == 5)


@pytest.mark.reporting
def test_get_weekly_trend_new_cases(input_df):
    """ Test if there is a 100% increase in cases in Europe between W5 and 6"""
    df = DataAnalysis(input_df).get_weekly_trend_new_cases()
    new_cases_trend_w6 = df.where(col('continent') == 'Europe').select('2022-W6').collect()[0][0]
    assert (new_cases_trend_w6 == '100.00')
