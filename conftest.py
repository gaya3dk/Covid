from os import path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def test_root_dir():
    root_dir = path.dirname(path.abspath(__file__))
    return root_dir


@pytest.fixture(scope="session")
def spark_session(tmpdir_factory):
    spark_session = (
        SparkSession.builder.appName("Covid - Unit test spark")
            .master("local[*]")
            .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
    # root_dir = path.dirname(path.abspath(__file__))
    # derby_log_file = root_dir + "/derby.log"
    # if path.exists(derby_log_file):
    #     remove(derby_log_file)
    # metastore_db_dir = root_dir + "/metastore_db"
    # rmtree(metastore_db_dir, ignore_errors=True)
