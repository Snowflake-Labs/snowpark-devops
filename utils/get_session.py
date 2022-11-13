from utils.snowflake_connection import SnowflakeConnection
from snowflake.snowpark import Session
import os
from typing import Optional


def session() -> Session:

    # if running in snowflake
    if SnowflakeConnection().connection:
        session = SnowflakeConnection().connection
    # if running locally with a config file
    elif os.path.exists('../config.py') or os.path.exists('config.py'):
        from config import snowpark_config
        SnowflakeConnection().connection = Session.builder.configs(snowpark_config).create()
    else:
        connection_parameters = {
            "account": os.environ["SNOWSQL_ACCOUNT"],
            "user": os.environ["SNOWSQL_USER"],
            "password": os.environ["SNOWSQL_PWD"],
            "role": os.environ["SNOWSQL_ROLE"],
            "warehouse": os.environ["SNOWSQL_WAREHOUSE"],
            "database": os.environ["SNOWSQL_DATABASE"],
            "schema": os.environ["SNOWSQL_SCHEMA"]
        }
        SnowflakeConnection().connection = Session.builder.configs(connection_parameters).create()
    if SnowflakeConnection().connection:
        return SnowflakeConnection().connection  # type: ignore
    else:
        raise Exception("Unable to create a session")
