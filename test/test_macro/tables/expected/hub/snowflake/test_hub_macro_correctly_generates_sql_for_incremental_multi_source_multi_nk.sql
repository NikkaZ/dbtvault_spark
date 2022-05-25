WITH row_rank_1 AS (
<<<<<<< HEAD
    SELECT CUSTOMER_PK, CUSTOMER_ID, CUSTOMER_NAME, LOAD_DATE, RECORD_SOURCE,
           ROW_NUMBER() OVER(
               PARTITION BY CUSTOMER_PK
               ORDER BY LOAD_DATE
           ) AS row_number
    FROM [DATABASE_NAME].[SCHEMA_NAME].raw_source
    WHERE CUSTOMER_PK IS NOT NULL
=======
    SELECT rr."CUSTOMER_PK", rr."CUSTOMER_ID", rr."CUSTOMER_NAME", rr."LOAD_DATE", rr."RECORD_SOURCE",
           ROW_NUMBER() OVER(
               PARTITION BY rr."CUSTOMER_PK"
               ORDER BY rr."LOAD_DATE"
           ) AS row_number
    FROM [DATABASE_NAME].[SCHEMA_NAME].raw_source AS rr
    WHERE rr."CUSTOMER_PK" IS NOT NULL
>>>>>>> dbtvault_update
    QUALIFY row_number = 1
),

row_rank_2 AS (
<<<<<<< HEAD
    SELECT CUSTOMER_PK, CUSTOMER_ID, CUSTOMER_NAME, LOAD_DATE, RECORD_SOURCE,
           ROW_NUMBER() OVER(
               PARTITION BY CUSTOMER_PK
               ORDER BY LOAD_DATE
           ) AS row_number
    FROM [DATABASE_NAME].[SCHEMA_NAME].raw_source_2
    WHERE CUSTOMER_PK IS NOT NULL
=======
    SELECT rr."CUSTOMER_PK", rr."CUSTOMER_ID", rr."CUSTOMER_NAME", rr."LOAD_DATE", rr."RECORD_SOURCE",
           ROW_NUMBER() OVER(
               PARTITION BY rr."CUSTOMER_PK"
               ORDER BY rr."LOAD_DATE"
           ) AS row_number
    FROM [DATABASE_NAME].[SCHEMA_NAME].raw_source_2 AS rr
    WHERE rr."CUSTOMER_PK" IS NOT NULL
>>>>>>> dbtvault_update
    QUALIFY row_number = 1
),

stage_union AS (
    SELECT * FROM row_rank_1
    UNION ALL
    SELECT * FROM row_rank_2
),

row_rank_union AS (
<<<<<<< HEAD
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY CUSTOMER_PK
               ORDER BY LOAD_DATE, RECORD_SOURCE ASC
           ) AS row_rank_number
    FROM stage_union
    WHERE CUSTOMER_PK IS NOT NULL
=======
    SELECT ru.*,
           ROW_NUMBER() OVER(
               PARTITION BY ru."CUSTOMER_PK"
               ORDER BY ru."LOAD_DATE", ru."RECORD_SOURCE" ASC
           ) AS row_rank_number
    FROM stage_union AS ru
    WHERE ru."CUSTOMER_PK" IS NOT NULL
>>>>>>> dbtvault_update
    QUALIFY row_rank_number = 1
),

records_to_insert AS (
<<<<<<< HEAD
    SELECT a.CUSTOMER_PK, a.CUSTOMER_ID, a.CUSTOMER_NAME, a.LOAD_DATE, a.RECORD_SOURCE
    FROM row_rank_union AS a
    LEFT JOIN [DATABASE_NAME].[SCHEMA_NAME].test_hub_macro_correctly_generates_sql_for_incremental_multi_source_multi_nk AS d
    ON a.CUSTOMER_PK = d.CUSTOMER_PK
    WHERE d.CUSTOMER_PK IS NULL
=======
    SELECT a."CUSTOMER_PK", a."CUSTOMER_ID", a."CUSTOMER_NAME", a."LOAD_DATE", a."RECORD_SOURCE"
    FROM row_rank_union AS a
    LEFT JOIN [DATABASE_NAME].[SCHEMA_NAME].test_hub_macro_correctly_generates_sql_for_incremental_multi_source_multi_nk AS d
    ON a."CUSTOMER_PK" = d."CUSTOMER_PK"
    WHERE d."CUSTOMER_PK" IS NULL
>>>>>>> dbtvault_update
)

SELECT * FROM records_to_insert