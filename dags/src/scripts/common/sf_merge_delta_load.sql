-- /*
-- Parameters:
-- - bronze_db: bronze Snowflake layer
-- - silver_db: silver Snowflake layer
-- - schema: Snowflake schema where objects (bronze and silver) will be loaded
-- - table: Table name (same for bronze and silver)
-- - merge_on: Logic used to merge data (e.g. tgt.k = upd.k)
-- */

-- -- CREATE TABLE IF NOT EXISTS {{silver_db}}.{{schema}}.{{table}} AS 
-- --   SELECT * FROM {{bronze_db}}.{{schema}}.{{table}};

-- -- MERGE INTO {{silver_db}}.{{schema}}.{{table}} tgt
-- --   USING (select * from {{bronze_db}}.{{schema}}.{{table}} where ingested_at > current_date - 3) upd
-- --   ON {{merge_on}}
-- --   WHEN MATCHED THEN UPDATE SET ALL
-- --   WHEN NOT MATCHED THEN INSERT ALL;

-- CREATE OR REPLACE TABLE {{database}}.{{schema}}.{{table}} AS SELECT 1 ID;