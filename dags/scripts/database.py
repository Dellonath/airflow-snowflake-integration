import pandas as pd
import logging
import tomllib
from dataclasses import dataclass, field
from sqlalchemy import create_engine, engine
from enum import Enum

DEV_DBCREDS: str = 'dags/creds/dev-dbcreds.toml'
PRD_DBCREDS: str = 'dags/creds/dbcreds.toml'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler() # stream to console
    ]
)

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
    query: str
    output: str
    file_format: FileFormat = field(default=FileFormat.PARQUET.value)

@dataclass
class DatabaseConnectionCredentials:
    drivername: DatabaseDriverName
    host: str
    username: str
    password: str
    port: int
    database: str

def establish_database_connection(db_conn_creds: DatabaseConnectionCredentials) -> create_engine:
    try:
        if db_conn_creds.drivername in (DatabaseDriverName.MYSQL.value, DatabaseDriverName.POSTGRES.value):
            db_engine: create_engine = create_engine(url=engine.URL.create(
                drivername=db_conn_creds.drivername,
                host=db_conn_creds.host,
                username=db_conn_creds.username,
                password=db_conn_creds.password,
                port=db_conn_creds.port,
                database=db_conn_creds.database
            )).connect()
        elif db_conn_creds.drivername == DatabaseDriverName.CACHE.value:
            raise NotImplementedError('Cache database engine is not yet implemented')
        logging.info(f"Connection to '{db_conn_creds.database}' database established successfully")
        return db_engine
    except Exception as e:
        logging.error(e)
        exit()


def save(dataframe: pd.DataFrame, db_task_params: DatabaseTaskParameters) -> None:
    if db_task_params.file_format == FileFormat.PARQUET.value:
        dataframe.to_parquet(db_task_params.output, index=False)
    elif db_task_params.file_format == FileFormat.CSV.value:
        dataframe.to_csv(db_task_params.output, index=False)

def transform(dataframe: pd.DataFrame) -> pd.DataFrame:
    # add any common transformation logic here
    return dataframe

class DatabaseConnector:

    @staticmethod
    def handler(dbcreds: str, query: str, output: str, file_format: str | None=None) -> None:
        with open(file=PRD_DBCREDS, mode='rb') as f:
            db_conn_params: dict = tomllib.load(f)
        _db_task_params: DatabaseTaskParameters = DatabaseTaskParameters(
            dbcreds=dbcreds,
            query=query,
            output=output,
            file_format=file_format
        )
        _db_connection: create_engine = establish_database_connection(
            DatabaseConnectionCredentials(
                **db_conn_params.get(_db_task_params.dbcreds)
            )
        )
        dataframe: pd.DataFrame = transform(
            pd.read_sql(sql=_db_task_params.query, con=_db_connection.connection)
        )
        save(dataframe=dataframe, db_task_params=_db_task_params)
