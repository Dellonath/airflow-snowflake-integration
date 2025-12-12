CREATE TABLE IF NOT EXISTS {{database}}.{{schema}}.{{table}}
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION=>'@{{database}}.{{schema}}.{{table}}',
                FILE_FORMAT=>'{{file_format}}',
                IGNORE_CASE=>false
            )
        )
    ) ENABLE_SCHEMA_EVOLUTION=true