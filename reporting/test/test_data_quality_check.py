import os

import pytest

from reporting.src.utils.data_io import DataIO
from reporting.src.utils.data_quality_check import DataQualityCheck

BASE_PATH = str(os.path.dirname(__file__)) + "/test_data/"


@pytest.fixture(scope="module")
def input_df(spark_session):
    """ Input dataframe to share in the session - valid data"""
    path = BASE_PATH
    filename="sampledata.csv"
    df = DataIO.read_csv_from_disk(path, filename, spark_session)
    return df


@pytest.fixture(scope="module")
def input_df2(spark_session):
    """ Input dataframe to share in the session- empty data"""
    path = BASE_PATH
    filename="sampledata2.csv"
    df = DataIO.read_csv_from_disk(path, filename, spark_session)
    return df


@pytest.fixture(scope="module")
def input_df3(spark_session):
    """ Input dataframe to share in the session - invalid columns"""
    path = BASE_PATH
    filename="sampledata3.csv"
    df = DataIO.read_csv_from_disk(path, filename, spark_session)
    return df


@pytest.mark.reporting
def test_expect_non_empty_data(input_df2):
    """ Assert that input data is not empty"""
    with pytest.raises(ValueError):
        DataQualityCheck.expect_non_empty_data(input_df2)


@pytest.mark.reporting
def test_expect_date_column(input_df2):
    """ Assert that input data has a date column"""
    with pytest.raises(Exception):
        DataQualityCheck.expect_date_column(input_df2)


@pytest.mark.reporting
def test_expect_fresh_data(input_df):
    """ Assert that input data freshness is utmost 1 day """
    with pytest.raises(ValueError):
        DataQualityCheck.expect_fresh_data(input_df)


@pytest.mark.reporting
def test_expect_valid_numeric_data(input_df3):
    """ Assert that input data freshness is utmost 1 day """
    with pytest.raises(ValueError):
        DataQualityCheck.expect_valid_numeric_data(input_df3)