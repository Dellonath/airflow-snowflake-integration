CREATE DYNAMIC TABLE IF NOT EXISTS {{database}}.{{schema}}.{{table}}
    TARGET_LAG = '30 minutes'
    REFRESH_MODE = FULL
    WAREHOUSE = {{warehouse}}
    AS SELECT {{columns}} FROM {{schema}}.{{table}}