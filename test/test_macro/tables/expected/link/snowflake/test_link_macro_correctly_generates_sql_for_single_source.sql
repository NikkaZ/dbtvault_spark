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
=======
    SELECT a."CUSTOMER_PK", a."ORDER_FK", a."BOOKING_FK", a."LOAD_DATE", a."RECORD_SOURCE"
>>>>>>> dbtvault_update
    FROM row_rank_1 AS a
)

SELECT * FROM records_to_insert