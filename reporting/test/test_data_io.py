import os

import pytest

from reporting.src.utils.data_io import DataIO
BASE_PATH = str(os.path.dirname(__file__)) + "/test_data/"


@pytest.mark.reporting
def test_read_csv_from_disk(spark_session):
    """ Assert that data  can be read from csv to spark dataframe"""
    path = BASE_PATH
    filename="sampledata.csv"
    actual_df = DataIO.read_csv_from_disk(path, filename, spark_session)
    assert (actual_df.count() == 4)


@pytest.mark.reporting
def test_read_csv_from_disk_invalid(spark_session):
    """ Assert that invalid path raises exception"""
    path = "invalid"
    filename="sampledata.csv"
    with pytest.raises(Exception):
        DataIO.read_csv_from_disk(path, filename, spark_session)