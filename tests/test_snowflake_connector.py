from typing import Dict
from pandas import DataFrame

from snopy import SnowflakeConnector


def test_valid_connection(sf_conn_valid: SnowflakeConnector):
    assert sf_conn_valid.connected
    assert sf_conn_valid.storage_integration is not None  # connector's api objects checking
    assert sf_conn_valid.cursor is not None


def test_get_environment(sf_conn_valid: SnowflakeConnector):
    current_environment = sf_conn_valid.get_environment()

    assert isinstance(current_environment, Dict)
    assert len(current_environment.items()) == 4
    assert current_environment["warehouse"] is None


def test_query_pd(sf_conn_valid: SnowflakeConnector, query_get_schemas: str):
    data = sf_conn_valid.query_pd(query_get_schemas)

    assert isinstance(data, DataFrame)
    assert len(data) == 9


def test_execute(sf_conn_valid: SnowflakeConnector, query_get_schemas: str):
    results = sf_conn_valid.execute(query_get_schemas, n=-1)

    assert len(results["results"]) == 9
    assert len(results["description"]) == 9  # metadata for each column
    assert results["statement"] == query_get_schemas


def test_set_environment(sf_conn_valid: SnowflakeConnector):
    SNOWFLAKE_DATABASE = "SNOWFLAKE"
    sf_conn_valid.set_environment(database=SNOWFLAKE_DATABASE)

    assert sf_conn_valid.get_environment()["database"] == SNOWFLAKE_DATABASE
