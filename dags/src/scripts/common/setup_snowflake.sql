CREATE FILE FORMAT IF NOT EXISTS {{bronze_db}}.{{schema}}.PARQUET_SCHEMA_EVOLUTION
  TYPE='PARQUET';

CREATE FILE FORMAT IF NOT EXISTS {{bronze_db}}.{{schema}}.CSV_SCHEMA_EVOLUTION
  TYPE='CSV'
  FIELD_DELIMITER=','
  PARSE_HEADER=true
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE
  ERROR_ON_COLUMN_COUNT_MISMATCH=false;

CREATE STAGE IF NOT EXISTS @{{bronze_db}}.{{schema}}.{{table}}
  FILE_FORMAT={{file_format}}_SCHEMA_EVOLUTION;

PUT {{path}}* @{{bronze_db}}.{{schema}}.{{table}};

CREATE TABLE IF NOT EXISTS {{silver_db}}.{{schema}}.{{table}}
  USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
        INFER_SCHEMA(
        LOCATION=>'@{{bronze_db}}.{{schema}}.{{table}}',
        FILE_FORMAT=>'{{file_format}}',
        IGNORE_CASE=>false
      )
    )
  ) ENABLE_SCHEMA_EVOLUTION=true;

MERGE INTO {{silver_db}}.{{schema}}.{{table}} tgt
  USING {{bronze_db}}.{{schema}}.{{table}} src
  ON tgt.k = src.k, 
  WHEN MATCHED THEN UPDATE SET ALL
  WHEN NOT MATCHED THEN INSERT ALL;