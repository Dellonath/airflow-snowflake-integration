/*
database: sandbox
schema: oliveirad
table: clients
file_format: {{file_format}}
extraction_files_path: {{bronze_path}}/crm_clients"
*/

CREATE STAGE IF NOT EXISTS @{{database}}.{{schema}}.{{table}}
  FILE_FORMAT={{file_format}}_SCHEMA_EVOLUTION;

PUT {{extraction_files_path}}* @{{database}}.{{schema}}.{{table}};

CREATE TABLE IF NOT EXISTS {{database}}.{{schema}}.{{table}}
  USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
        INFER_SCHEMA(
        LOCATION=>'@{{database}}.{{schema}}.{{table}}',
        FILE_FORMAT=>'{{database}}.{{schema}}.{{file_format}}_SCHEMA_EVOLUTION',
        IGNORE_CASE=>false
      )
    )
  ) ENABLE_SCHEMA_EVOLUTION=true;

COPY INTO {{database}}.{{schema}}.{{table}}
  FROM @{{database}}.{{schema}}.{{table}}
  ON_ERROR='SKIP_FILE_1%'
  MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
  FILE_FORMAT={{database}}.{{schema}}.{{file_format}}_SCHEMA_EVOLUTION;

CREATE OR REPLACE TABLE {{database}}.{{schema}}.{{table}} AS SELECT 1 ID;