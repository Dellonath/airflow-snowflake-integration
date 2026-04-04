import logging
from typing import Any
from pydantic import ConfigDict, dataclasses, Field
import snowflake.connector
from ..utils.load_toml_creds import load_toml_creds

@dataclasses.dataclass(config=ConfigDict(extra='ignore'))
class TaskParameters:
    dbcreds: str = Field(description='Snowflake connection section name in TOML file. With all necessary parameters for SF connection')
    sql: str = Field(description='Query or the SQL file path to be executed')
    sql_params: dict[str, Any] | None = Field(default=None, description='Args for replacing variables between double curly braces in SQL file scripts')

def execute_query_or_sql_file(conn: snowflake.connector, sql: str) -> None:
    with conn.cursor() as cursor:
        logging.info(f'Executing query: {sql}')
        cursor.execute(sql)

def __replace_args_placeholders_by_args(sql: str, sql_params: dict[str, Any] | None) -> str:
    if sql_params:
        for param, value in sql_params.items():
            # replace all {{keys}} references with the respectivee values passed in sql_params
            # e.g. sql: SELECT {{param}}, sql_params={'param': 'test'} -> SELECT test
            sql = sql.replace(f'{{{{{param}}}}}', value)
    return sql

def format_sql(sql: str, sql_params: dict[str, Any] | None) -> str:
    # check if the parameter sql is a sql file path or string sql script
    if sql.endswith('.sql'):
        with open(file=sql, mode='r') as f:
            sql = f.read()
    sql: str = __replace_args_placeholders_by_args(sql=sql, sql_params=sql_params)    
    return sql

def handler(**kwargs) -> None:
    params: TaskParameters = TaskParameters(**kwargs)
    # sql = format_sql(sql=params.sql, sql_params=params.sql_params)
    # conn = snowflake.connector.connect(
    #     **load_toml_creds().get(params.dbcreds),
    #     client_session_keep_alive=True
    # )
    # execute_query_or_sql_file(conn=conn, sql=sql)