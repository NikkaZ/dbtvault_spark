WITH row_rank_1 AS (
<<<<<<< HEAD
    SELECT CUSTOMER_PK, ORDER_FK, BOOKING_FK, LOAD_DATE, RECORD_SOURCE,
           ROW_NUMBER() OVER(
               PARTITION BY CUSTOMER_PK
               ORDER BY LOAD_DATE
           ) AS row_number
    FROM [DATABASE_NAME].[SCHEMA_NAME].raw_source
    WHERE CUSTOMER_PK IS NOT NULL
    AND ORDER_FK IS NOT NULL
    AND BOOKING_FK IS NOT NULL
=======
    SELECT rr."CUSTOMER_PK", rr."ORDER_FK", rr."BOOKING_FK", rr."LOAD_DATE", rr."RECORD_SOURCE",
           ROW_NUMBER() OVER(
               PARTITION BY rr."CUSTOMER_PK"
               ORDER BY rr."LOAD_DATE"
           ) AS row_number
    FROM [DATABASE_NAME].[SCHEMA_NAME].raw_source AS rr
    WHERE rr."CUSTOMER_PK" IS NOT NULL
    AND rr."ORDER_FK" IS NOT NULL
    AND rr."BOOKING_FK" IS NOT NULL
>>>>>>> dbtvault_update
    QUALIFY row_number = 1
),

records_to_insert AS (
<<<<<<< HEAD
    SELECT a.CUSTOMER_PK, a.ORDER_FK, a.BOOKING_FK, a.LOAD_DATE, a.RECORD_SOURCE
    FROM row_rank_1 AS a
    LEFT JOIN [DATABASE_NAME].[SCHEMA_NAME].test_link_macro_correctly_generates_sql_for_incremental_single_source AS d
    ON a.CUSTOMER_PK = d.CUSTOMER_PK
    WHERE d.CUSTOMER_PK IS NULL
=======
    SELECT a."CUSTOMER_PK", a."ORDER_FK", a."BOOKING_FK", a."LOAD_DATE", a."RECORD_SOURCE"
    FROM row_rank_1 AS a
    LEFT JOIN [DATABASE_NAME].[SCHEMA_NAME].test_link_macro_correctly_generates_sql_for_incremental_single_source AS d
    ON a."CUSTOMER_PK" = d."CUSTOMER_PK"
    WHERE d."CUSTOMER_PK" IS NULL
>>>>>>> dbtvault_update
)

SELECT * FROM records_to_insert