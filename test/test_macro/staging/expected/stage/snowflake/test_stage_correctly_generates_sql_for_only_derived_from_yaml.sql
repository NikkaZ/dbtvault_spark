WITH source_data AS (

    SELECT

<<<<<<< HEAD
    BOOKING_FK,
    ORDER_FK,
    CUSTOMER_PK,
    CUSTOMER_ID,
    LOAD_DATE,
    RECORD_SOURCE,
    CUSTOMER_DOB,
    CUSTOMER_NAME,
    NATIONALITY,
    PHONE,
    TEST_COLUMN_2,
    TEST_COLUMN_3,
    TEST_COLUMN_4,
    TEST_COLUMN_5,
    TEST_COLUMN_6,
    TEST_COLUMN_7,
    TEST_COLUMN_8,
    TEST_COLUMN_9,
    BOOKING_DATE
=======
    "BOOKING_FK",
    "ORDER_FK",
    "CUSTOMER_PK",
    "CUSTOMER_ID",
    "LOAD_DATE",
    "RECORD_SOURCE",
    "CUSTOMER_DOB",
    "CUSTOMER_NAME",
    "NATIONALITY",
    "PHONE",
    "TEST_COLUMN_2",
    "TEST_COLUMN_3",
    "TEST_COLUMN_4",
    "TEST_COLUMN_5",
    "TEST_COLUMN_6",
    "TEST_COLUMN_7",
    "TEST_COLUMN_8",
    "TEST_COLUMN_9",
    "BOOKING_DATE"
>>>>>>> dbtvault_update

    FROM [DATABASE_NAME].[SCHEMA_NAME].raw_source
),

derived_columns AS (

    SELECT

<<<<<<< HEAD
    BOOKING_FK,
    ORDER_FK,
    CUSTOMER_PK,
    CUSTOMER_ID,
    LOAD_DATE,
    RECORD_SOURCE,
    CUSTOMER_DOB,
    CUSTOMER_NAME,
    NATIONALITY,
    PHONE,
    TEST_COLUMN_2,
    TEST_COLUMN_3,
    TEST_COLUMN_4,
    TEST_COLUMN_5,
    TEST_COLUMN_6,
    TEST_COLUMN_7,
    TEST_COLUMN_8,
    TEST_COLUMN_9,
    BOOKING_DATE,
    'STG_BOOKING' AS SOURCE,
    LOAD_DATE AS EFFECTIVE_FROM
=======
    "BOOKING_FK",
    "ORDER_FK",
    "CUSTOMER_PK",
    "CUSTOMER_ID",
    "LOAD_DATE",
    "RECORD_SOURCE",
    "CUSTOMER_DOB",
    "CUSTOMER_NAME",
    "NATIONALITY",
    "PHONE",
    "TEST_COLUMN_2",
    "TEST_COLUMN_3",
    "TEST_COLUMN_4",
    "TEST_COLUMN_5",
    "TEST_COLUMN_6",
    "TEST_COLUMN_7",
    "TEST_COLUMN_8",
    "TEST_COLUMN_9",
    "BOOKING_DATE",
    'STG_BOOKING' AS "SOURCE",
    "LOAD_DATE" AS "EFFECTIVE_FROM"
>>>>>>> dbtvault_update

    FROM source_data
),

columns_to_select AS (

    SELECT

<<<<<<< HEAD
    SOURCE,
    EFFECTIVE_FROM
=======
    "SOURCE",
    "EFFECTIVE_FROM"
>>>>>>> dbtvault_update

    FROM derived_columns
)

SELECT * FROM columns_to_select