import os
import pytest

from snopy import SnowflakeConnector

SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_DATABASE_SAMPLE = "SNOWFLAKE_SAMPLE_DATA"


@pytest.fixture()
def sf_conn_valid() -> SnowflakeConnector:
    return SnowflakeConnector(
        username=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE_SAMPLE,
    )


@pytest.fixture()
def query_get_schemas() -> str:
    return "SHOW SCHEMAS;"
