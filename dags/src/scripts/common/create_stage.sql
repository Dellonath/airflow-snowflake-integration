CREATE STAGE IF NOT EXISTS @{{database}}.{{schema}}.{{table}}
  FILE_FORMAT={{file_format}}_SCHEMA_EVOLUTION;

PUT {{path}}* @{{database}}.{{schema}}.{{table}};