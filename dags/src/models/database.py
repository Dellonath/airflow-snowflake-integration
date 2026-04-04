import logging
from datetime import datetime
from typing import Iterator
from pydantic import ConfigDict, dataclasses, Field
from pathlib import Path
from enum import Enum
import pandas as pd
from sqlalchemy import create_engine, engine
from ..utils.load_toml_creds import load_toml_creds
from airflow.models.dagrun import DagRun

class DatabaseDriverName(Enum):
    MYSQL = 'mysql+pymysql'
    POSTGRES = 'postgresql+psycopg2'
    CACHE = 'com.intersys.jdbc.CacheDriver'

class FileFormat(Enum):
    PARQUET = 'parquet'
    CSV = 'csv'

@dataclasses.dataclass
class DatabaseConnectionCredentials:
    drivername: DatabaseDriverName = Field(description='Driver name to identify the database driver')
    host: str = Field(description='Database host')
    username: str = Field(description='Database username')
    password: str = Field(description='Database username password')
    port: int = Field(description='Database port')
    database: str | None = Field(default=None, description='Database to be used in during connection runtime')

@dataclasses.dataclass(config=ConfigDict(extra='ignore'))
class TaskParameters:
    sql: str = Field(description='Query or the SQL file path to be executed')
    dbcreds: str = Field(description='Database connection section name in TOML file. With all necessary parameters for connection')
    data_interval_start: datetime = Field(description='The start interval of the Dag execution')
    output: Path = Field(default_factory=Path, description='Extraction files path where the files will be saved')
    file_format: FileFormat = Field(default=FileFormat.PARQUET, description='Extraction files final format')
    # timestamp_column: str = Field(description='Column name to be used during incremental load based on data_interval_start')
    chunk_size: int | None = Field(default=None, description='Size (number of records) of each chunk/file during the extraction')
    full_load: bool = Field(default=False, description='Flag to indicate if the extraction is a full load or incremental load')

def establish_database_connection(db_creds: DatabaseConnectionCredentials) -> create_engine:
    if db_creds.drivername in (DatabaseDriverName.MYSQL, DatabaseDriverName.POSTGRES):
        db_engine: create_engine = create_engine(url=engine.URL.create(
            drivername=db_creds.drivername.value,
            host=db_creds.host,
            username=db_creds.username,
            password=db_creds.password,
            port=db_creds.port,
            database=db_creds.database
        )).connect().connection
    elif db_creds.drivername == DatabaseDriverName.CACHE:
        raise NotImplementedError('Cache database engine is not yet implemented')
    else:
        raise ValueError(f"The engine '{db_creds.drivername}' is unknown")
    logging.info(f"Connection to '{db_creds.database}' database established successfully")
    return db_engine

def save_extraction_file(dataframe: pd.DataFrame, output: Path, file_name: str, file_format: FileFormat) -> None:
    output.mkdir(parents=True, exist_ok=True)
    file_name: str = f'{file_name}.{file_format.value}'
    if file_format == FileFormat.PARQUET:
        dataframe.to_parquet(path=output/file_name, index=False)
    elif file_format == FileFormat.CSV:
        dataframe.to_csv(path_or_buf=output/file_name, index=False)
    else:
        raise ValueError(f"Extraction file format '{file_format}' is invalid, please check file format task parameter")

def extract_from_database(sql: str, conn: create_engine, chunk_size: int | None) -> None:
    sql_result = pd.read_sql(
        sql=sql,
        con=conn,
        chunksize=chunk_size
    )
    data_iterator: Iterator = sql_result if chunk_size else [sql_result]
    return data_iterator

def handler(**kwargs) -> None:
    params: TaskParameters = TaskParameters(
        full_load=kwargs.get('dag_run').conf.get('full_load', False),
        **kwargs
    )
    conn: create_engine = establish_database_connection(
        db_creds=DatabaseConnectionCredentials(
            **load_toml_creds().get(params.dbcreds)
        )
    )
    data: Iterator = extract_from_database(
        sql=params.sql, 
        conn=conn, 
        chunk_size=params.chunk_size
    )
    for i, chunk in enumerate(data):
        # only add suffix if we are actually chunking
        suffix: str = f'_{i}' if params.chunk_size else ''

        # setting ingestion timestamp to be used in downstream processes for delta loading
        chunk['ingested_at'] = params.data_interval_start

        save_extraction_file(
            dataframe=chunk,
            output=params.output,
            file_name=params.data_interval_start.strftime('%Y%m%d%H%M%S') + suffix,
            file_format=params.file_format
        )