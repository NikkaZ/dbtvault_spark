CAST(SHA2_BINARY(NULLIF(CONCAT_WS('||',
    IFNULL(NULLIF(UPPER(TRIM(CAST("CUSTOMER_ID" AS VARCHAR))), ''), '^^'),
    IFNULL(NULLIF(UPPER(TRIM(CAST("PHONE" AS VARCHAR))), ''), '^^'),
    IFNULL(NULLIF(UPPER(TRIM(CAST("DOB" AS VARCHAR))), ''), '^^')
), '^^||^^||^^')) AS BINARY(32)) AS "CUSTOMER_PK"