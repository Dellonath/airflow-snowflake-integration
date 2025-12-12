import logging
from typing import Any
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dataclasses import dataclass, field
from ..utils.load_toml_creds import load_toml_creds

@dataclass
class SnowflakeTaskParameters:
    dbcreds: str
    sql: str
    sql_params: dict[str, Any] | None = field(default=None)

@dataclass
class SnowflakeConnectionCredentials:
    authenticator: str
    user: str
    role: str
    account: str
    private_key_file: str
    private_key_file_pwd: str
    warehouse: str

def execute_query(conn: snowflake.connector, sql: str) -> None:
    with conn.cursor() as cursor:
        logging.info(f'Executing query: {sql}')
        cursor.execute(sql)

def format_sql(sql: str, sql_params: dict[str, Any] | None) -> None:
    if sql.endswith('.sql') and sql_params is None:
        Exception("Parameter 'sql_params' must be passed only when 'sql' is a SQL file path")

    # check if the parameter sql is a sql file path or string sql script
    if sql.endswith('.sql'):
        with open(file=sql, mode='r') as f:
            sql = f.read()
        for param, value in sql_params.items():
            # replace all {{keys}} references with the respectivee values passed in sql_params
            # e.g. sql: SELECT {{param}}, sql_params={'param': 'test'} -> SELECT test
            sql = sql.replace(f'{{{{{param}}}}}', value)
    return sql

class SnowflakeConnector:

    @staticmethod
    def handler(dbcreds: str, sql: str, sql_params: dict[str, Any] | None = None, *args, **kwargs) -> None:
        sf_task_params: SnowflakeTaskParameters = SnowflakeTaskParameters(
            dbcreds=dbcreds,
            sql=sql
        )

        sql = format_sql(sql=sql, sql_params=sql_params)

        sf_connection = snowflake.connector.connect(
            **load_toml_creds().get(sf_task_params.dbcreds),
            client_session_keep_alive=True
        )

        execute_query(conn=sf_connection, sql=sql)