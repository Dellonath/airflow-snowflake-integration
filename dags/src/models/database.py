import logging
from enum import Enum
import datetime
import pandas as pd
from typing import Iterator
from dataclasses import dataclass
from sqlalchemy import create_engine, engine
from pathlib import Path
from ..utils.load_toml_creds import load_toml_creds

class DatabaseDriverName(Enum):
    MYSQL = 'mysql+pymysql'
    POSTGRES = 'postgresql+psycopg2'
    CACHE = 'com.intersys.jdbc.CacheDriver'

class FileFormat(Enum):
    PARQUET = 'parquet'
    CSV = 'csv'

@dataclass
class DatabaseTaskParameters:
    dbcreds: str
    sql: str
    output: Path
    file_format: FileFormat
    chunk_size: int

@dataclass
class DatabaseConnectionCredentials:
    drivername: DatabaseDriverName
    host: str
    username: str
    password: str
    port: int
    database: str

def now():
    return datetime.datetime.now()

def establish_database_connection(db_conn_creds: DatabaseConnectionCredentials) -> create_engine:

    if db_conn_creds.drivername in (DatabaseDriverName.MYSQL.value, DatabaseDriverName.POSTGRES.value):
        db_engine: create_engine = create_engine(url=engine.URL.create(
            drivername=db_conn_creds.drivername,
            host=db_conn_creds.host,
            username=db_conn_creds.username,
            password=db_conn_creds.password,
            port=db_conn_creds.port,
            database=db_conn_creds.database
        )).connect().connection
    elif db_conn_creds.drivername == DatabaseDriverName.CACHE.value:
        raise NotImplementedError('Cache database engine is not yet implemented')
    logging.info(f"Connection to '{db_conn_creds.database}' database established successfully")

    return db_engine

def save(dataframe: pd.DataFrame, output: Path, file_name: str) -> None:
    output.mkdir(parents=True, exist_ok=True)
    file_format = file_name.split('.')[-1]
    if file_format == FileFormat.PARQUET.value:
        dataframe.to_parquet(path=output/file_name, index=False)
    elif file_format == FileFormat.CSV.value:
        dataframe.to_csv(path_or_buf=output/file_name, index=False)
    else:
        raise ValueError(f"Extraction file format '{file_format}' is invalid, please check file format task parameter")

def extract_from_database(db_task_params: DatabaseTaskParameters, con: create_engine) -> None:
    sql_result = pd.read_sql(
        sql=db_task_params.sql, 
        con=con, 
        chunksize=db_task_params.chunk_size
    )
    data_iterator: Iterator = sql_result if db_task_params.chunk_size else [sql_result]
    extraction_ts = now().strftime('%Y%m%d%H%M%S')
    for i, chunk in enumerate(data_iterator):
        # only add suffix if we are actually chunking
        suffix = f'_{i}' if db_task_params.chunk_size else ''
        save(
            dataframe=chunk,
            output=db_task_params.output,
            file_name=f"{extraction_ts}{suffix}.{db_task_params.file_format}"
        )


class DatabaseConnector:

    @staticmethod
    def handler(dbcreds: str, sql: str, output: Path, file_format: FileFormat = FileFormat.PARQUET.value, chunk_size: int | None = None) -> None:
        db_task_params: DatabaseTaskParameters = DatabaseTaskParameters(
            dbcreds=dbcreds,
            sql=sql,
            output=Path(output),
            file_format=file_format,
            chunk_size=chunk_size
        )
        db_connection: create_engine = establish_database_connection(
            DatabaseConnectionCredentials(
                **load_toml_creds().get(db_task_params.dbcreds)
            )
        )
        extract_from_database(db_task_params=db_task_params, con=db_connection)