from behave import fixture


<<<<<<< HEAD
@fixture
def multi_active_satellite(context):
=======
# Snowflake


@fixture
def multi_active_satellite_snowflake(context):
>>>>>>> dbtvault_update
    """
    Define the structures and metadata to load multi active satellites
    """
    context.vault_structure_type = "ma_sat"

    context.hashed_columns = {
        "STG_CUSTOMER": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_NAME"]}
        }
    }

    context.derived_columns = {
        "STG_CUSTOMER": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        }
    }

<<<<<<< HEAD
=======
    context.stage_columns = {
        "RAW_STAGE":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"]
    }

>>>>>>> dbtvault_update
    context.vault_structure_columns = {
        "MULTI_ACTIVE_SATELLITE": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
<<<<<<< HEAD
=======
        "MULTI_ACTIVE_SATELLITE_HD_ALIAS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": {"source_column": "HASHDIFF", "alias": "CUSTOMER_HASHDIFF"},
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
>>>>>>> dbtvault_update
        "MULTI_ACTIVE_SATELLITE_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        }
    }

    context.seed_config = {
        "RAW_STAGE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "NUMBER(38, 0)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "RAW_STAGE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "NUMBER(38, 0)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
        "RAW_STAGE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "NUMBER(38, 0)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "RAW_STAGE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "NUMBER(38, 0)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
<<<<<<< HEAD
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "+column_types": {
=======
        "MULTI_ACTIVE_SATELLITE_HD_ALIAS": {
            "column_types": {
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "CUSTOMER_HASHDIFF": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATE": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        }
    }


@fixture
<<<<<<< HEAD
def multi_active_satellite_cycle(context):
=======
def multi_active_satellite_cycle_snowflake(context):
>>>>>>> dbtvault_update
    """
    Define the structures and metadata to perform load cycles for multi active satellites
    """
    context.vault_structure_type = "ma_sat"

    context.hashed_columns = {
        "STG_CUSTOMER": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_NAME"]}
        }
    }

    context.derived_columns = {
        "STG_CUSTOMER": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        }
    }

    context.stage_columns = {
        "RAW_STAGE":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"]
    }

    context.vault_structure_columns = {
        "MULTI_ACTIVE_SATELLITE": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        }
    }

    context.seed_config = {
        "RAW_STAGE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "NUMBER(38, 0)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "RAW_STAGE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "NUMBER(38, 0)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
        "RAW_STAGE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "NUMBER(38, 0)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "RAW_STAGE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "NUMBER(38, 0)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "CUSTOMER_PHONE": "VARCHAR",
                "CUSTOMER_NAME": "VARCHAR",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "CUSTOMER_PHONE": "VARCHAR",
                "CUSTOMER_NAME": "VARCHAR",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "CUSTOMER_PHONE": "VARCHAR",
                "CUSTOMER_NAME": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "CUSTOMER_PHONE": "VARCHAR",
                "CUSTOMER_NAME": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR",
                "CUSTOMER_PHONE": "VARCHAR",
                "EXTENSION": "NUMBER(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR"
            }
        }
    }

<<<<<<< HEAD
    @fixture
    def multi_active_satellite_bigquery(context):
        """
        Define the structures and metadata to load multi active satellites
        """
        context.vault_structure_type = "ma_sat"

        context.hashed_columns = {
            "STG_CUSTOMER": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_TS": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_NO_CDK_HASHDIFF": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_TWO_CDK": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
            },
            "STG_CUSTOMER_TWO_CDK_TS": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
            },
            "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_NAME"]}
            }
        }

        context.derived_columns = {
            "STG_CUSTOMER": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_TS": {
                "EFFECTIVE_FROM": "LOAD_DATETIME"
            },
            "STG_CUSTOMER_NO_CDK_HASHDIFF": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_TWO_CDK": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_TWO_CDK_TS": {
                "EFFECTIVE_FROM": "LOAD_DATETIME"
            },
            "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            }
        }

        context.vault_structure_columns = {
            "MULTI_ACTIVE_SATELLITE": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TS": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATETIME",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATETIME",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            }
        }

        context.seed_config = {
            "RAW_STAGE": {
                "+column_types": {
                    "CUSTOMER_ID": "NUMERIC",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "RAW_STAGE_TS": {
                "+column_types": {
                    "CUSTOMER_ID": "NUMERIC",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "LOAD_DATETIME": "DATETIME",
                    "SOURCE": "STRING"
                }
            },
            "RAW_STAGE_TWO_CDK": {
                "+column_types": {
                    "CUSTOMER_ID": "NUMERIC",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "RAW_STAGE_TWO_CDK_TS": {
                "+column_types": {
                    "CUSTOMER_ID": "NUMERIC",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "LOAD_DATETIME": "DATETIME",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TS": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATETIME",
                    "LOAD_DATETIME": "DATETIME",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATETIME",
                    "LOAD_DATETIME": "DATETIME",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            }
        }

    @fixture
    def multi_active_satellite_cycle_bigquery(context):
        """
        Define the structures and metadata to perform load cycles for multi active satellites
        """
        context.vault_structure_type = "ma_sat"

        context.hashed_columns = {
            "STG_CUSTOMER": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_TS": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_NO_CDK_HASHDIFF": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_TWO_CDK": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
            },
            "STG_CUSTOMER_TWO_CDK_TS": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
            },
            "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
            },
            "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
                "CUSTOMER_PK": "CUSTOMER_ID",
                "HASHDIFF": {"is_hashdiff": True,
                             "columns": ["CUSTOMER_NAME"]}
            }
        }

        context.derived_columns = {
            "STG_CUSTOMER": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_TS": {
                "EFFECTIVE_FROM": "LOAD_DATETIME"
            },
            "STG_CUSTOMER_NO_CDK_HASHDIFF": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_TWO_CDK": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_TWO_CDK_TS": {
                "EFFECTIVE_FROM": "LOAD_DATETIME"
            },
            "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            },
            "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
                "EFFECTIVE_FROM": "LOAD_DATE"
            }
        }

        context.stage_columns = {
            "RAW_STAGE":
                ["CUSTOMER_ID",
                 "CUSTOMER_NAME",
                 "CUSTOMER_PHONE",
                 "EFFECTIVE_FROM",
                 "LOAD_DATE",
                 "SOURCE"],

            "RAW_STAGE_TS":
                ["CUSTOMER_ID",
                 "CUSTOMER_NAME",
                 "CUSTOMER_PHONE",
                 "EFFECTIVE_FROM",
                 "LOAD_DATETIME",
                 "SOURCE"],

            "RAW_STAGE_TWO_CDK":
                ["CUSTOMER_ID",
                 "CUSTOMER_NAME",
                 "CUSTOMER_PHONE",
                 "EXTENSION",
                 "EFFECTIVE_FROM",
                 "LOAD_DATE",
                 "SOURCE"],

            "RAW_STAGE_TWO_CDK_TS":
                ["CUSTOMER_ID",
                 "CUSTOMER_NAME",
                 "CUSTOMER_PHONE",
                 "EXTENSION",
                 "EFFECTIVE_FROM",
                 "LOAD_DATETIME",
                 "SOURCE"]
        }

        context.vault_structure_columns = {
            "MULTI_ACTIVE_SATELLITE": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TS": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATETIME",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATETIME",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
                "src_pk": "CUSTOMER_PK",
                "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
                "src_payload": ["CUSTOMER_NAME"],
                "src_hashdiff": "HASHDIFF",
                "src_eff": "EFFECTIVE_FROM",
                "src_ldts": "LOAD_DATE",
                "src_source": "SOURCE"
            }
        }

        context.seed_config = {
            "RAW_STAGE": {
                "+column_types": {
                    "CUSTOMER_ID": "NUMERIC",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "RAW_STAGE_TS": {
                "+column_types": {
                    "CUSTOMER_ID": "NUMERIC",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EFFECTIVE_FROM": "DATETIME",
                    "LOAD_DATETIME": "DATETIME",
                    "SOURCE": "STRING"
                }
            },
            "RAW_STAGE_TWO_CDK": {
                "+column_types": {
                    "CUSTOMER_ID": "NUMERIC",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "RAW_STAGE_TWO_CDK_TS": {
                "+column_types": {
                    "CUSTOMER_ID": "NUMERIC",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "EFFECTIVE_FROM": "DATETIME",
                    "LOAD_DATETIME": "DATETIME",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "HASHDIFF": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TS": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "HASHDIFF": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "EFFECTIVE_FROM": "DATETIME",
                    "LOAD_DATETIME": "DATETIME",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "HASHDIFF": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "EXTENSION": "NUMERIC",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "HASHDIFF": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "EXTENSION": "NUMERIC",
                    "EFFECTIVE_FROM": "DATETIME",
                    "LOAD_DATETIME": "DATETIME",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            },
            "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
                "+column_types": {
                    "CUSTOMER_PK": "STRING",
                    "CUSTOMER_NAME": "STRING",
                    "CUSTOMER_PHONE": "STRING",
                    "EXTENSION": "NUMERIC",
                    "HASHDIFF": "STRING",
                    "EFFECTIVE_FROM": "DATE",
                    "LOAD_DATE": "DATE",
                    "SOURCE": "STRING"
                }
            }
        }
=======

# BigQuery
>>>>>>> dbtvault_update


@fixture
def multi_active_satellite_bigquery(context):
    """
    Define the structures and metadata to load multi active satellites
    """
    context.vault_structure_type = "ma_sat"

    context.hashed_columns = {
        "STG_CUSTOMER": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_NAME"]}
        }
    }

    context.derived_columns = {
        "STG_CUSTOMER": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        }
    }

<<<<<<< HEAD
=======
    context.stage_columns = {
        "RAW_STAGE":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"]
    }

>>>>>>> dbtvault_update
    context.vault_structure_columns = {
        "MULTI_ACTIVE_SATELLITE": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
<<<<<<< HEAD
=======
        "MULTI_ACTIVE_SATELLITE_HD_ALIAS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": {"source_column": "HASHDIFF", "alias": "CUSTOMER_HASHDIFF"},
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
>>>>>>> dbtvault_update
        "MULTI_ACTIVE_SATELLITE_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        }
    }

    context.seed_config = {
        "RAW_STAGE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "RAW_STAGE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "STRING"
            }
        },
        "RAW_STAGE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "RAW_STAGE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "STRING"
            }
        },
<<<<<<< HEAD
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "+column_types": {
=======
        "MULTI_ACTIVE_SATELLITE_HD_ALIAS": {
            "column_types": {
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "CUSTOMER_HASHDIFF": "STRING",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATE": "DATETIME",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        }
    }


@fixture
def multi_active_satellite_cycle_bigquery(context):
    """
    Define the structures and metadata to perform load cycles for multi active satellites
    """
    context.vault_structure_type = "ma_sat"

    context.hashed_columns = {
        "STG_CUSTOMER": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_NAME"]}
        }
    }

    context.derived_columns = {
        "STG_CUSTOMER": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        }
    }

    context.stage_columns = {
        "RAW_STAGE":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"]
    }

    context.vault_structure_columns = {
        "MULTI_ACTIVE_SATELLITE": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        }
    }

    context.seed_config = {
        "RAW_STAGE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "RAW_STAGE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "STRING"
            }
        },
        "RAW_STAGE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "RAW_STAGE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "HASHDIFF": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "CUSTOMER_NAME": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "HASHDIFF": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "CUSTOMER_NAME": "STRING",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "HASHDIFF": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "CUSTOMER_NAME": "STRING",
                "EXTENSION": "NUMERIC",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "HASHDIFF": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "CUSTOMER_NAME": "STRING",
                "EXTENSION": "NUMERIC",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "STRING",
                "CUSTOMER_NAME": "STRING",
                "CUSTOMER_PHONE": "STRING",
                "EXTENSION": "NUMERIC",
                "HASHDIFF": "STRING",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "STRING"
            }
        }
    }


<<<<<<< HEAD
=======
# SQLServer


>>>>>>> dbtvault_update
@fixture
def multi_active_satellite_sqlserver(context):
    """
    Define the structures and metadata to load multi active satellites
    """
    context.vault_structure_type = "ma_sat"

    context.hashed_columns = {
        "STG_CUSTOMER": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": "CUSTOMER_NAME"}
        },
        "STG_CUSTOMER_TWO_CDK": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": "CUSTOMER_NAME"}
        }
    }

    context.derived_columns = {
        "STG_CUSTOMER": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        }
    }

<<<<<<< HEAD
=======
    context.stage_columns = {
        "RAW_STAGE":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"]
    }

>>>>>>> dbtvault_update
    context.vault_structure_columns = {
        "MULTI_ACTIVE_SATELLITE": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
<<<<<<< HEAD
=======
        "MULTI_ACTIVE_SATELLITE_HD_ALIAS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": {"source_column": "HASHDIFF", "alias": "CUSTOMER_HASHDIFF"},
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
>>>>>>> dbtvault_update
        "MULTI_ACTIVE_SATELLITE_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        }
    }

    context.seed_config = {
        "RAW_STAGE": {
<<<<<<< HEAD
            "+column_types": {
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
=======
            "column_types": {
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EFFECTIVE_FROM": "DATE",
>>>>>>> dbtvault_update
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "RAW_STAGE_TS": {
<<<<<<< HEAD
            "+column_types": {
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "LOAD_DATETIME": "DATETIME",
=======
            "column_types": {
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "LOAD_DATETIME": "DATETIME2",
>>>>>>> dbtvault_update
                "SOURCE": "VARCHAR(50)"
            }
        },
        "RAW_STAGE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
<<<<<<< HEAD
=======
                "EFFECTIVE_FROM": "DATE",
>>>>>>> dbtvault_update
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "RAW_STAGE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
<<<<<<< HEAD
                "LOAD_DATETIME": "DATETIME",
=======
                "LOAD_DATETIME": "DATETIME2",
>>>>>>> dbtvault_update
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "HASHDIFF": "BINARY(16)",
<<<<<<< HEAD
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
=======
                "EFFECTIVE_FROM": "DATETIME2",
                "LOAD_DATETIME": "DATETIME2",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_HD_ALIAS": {
            "column_types": {
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "CUSTOMER_HASHDIFF": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATETIME2",
                "LOAD_DATE": "DATETIME2",
>>>>>>> dbtvault_update
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
                "HASHDIFF": "BINARY(16)",
<<<<<<< HEAD
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
=======
                "EFFECTIVE_FROM": "DATETIME2",
                "LOAD_DATETIME": "DATETIME2",
>>>>>>> dbtvault_update
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        }
    }


@fixture
def multi_active_satellite_cycle_sqlserver(context):
    """
    Define the structures and metadata to perform load cycles for multi active satellites
    """
    context.vault_structure_type = "ma_sat"

    context.hashed_columns = {
        "STG_CUSTOMER": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": "CUSTOMER_NAME"}
        },
        "STG_CUSTOMER_TWO_CDK": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_PHONE", "CUSTOMER_NAME", "EXTENSION"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": ["CUSTOMER_ID", "CUSTOMER_NAME"]}
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "CUSTOMER_PK": "CUSTOMER_ID",
            "HASHDIFF": {"is_hashdiff": True,
                         "columns": "CUSTOMER_NAME"}
        }
    }

    context.derived_columns = {
        "STG_CUSTOMER": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_TS": {
            "EFFECTIVE_FROM": "LOAD_DATETIME"
        },
        "STG_CUSTOMER_TWO_CDK_NO_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        },
        "STG_CUSTOMER_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "EFFECTIVE_FROM": "LOAD_DATE"
        }
    }

    context.stage_columns = {
        "RAW_STAGE":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATE",
             "SOURCE"],

        "RAW_STAGE_TWO_CDK_TS":
            ["CUSTOMER_ID",
             "CUSTOMER_NAME",
             "CUSTOMER_PHONE",
             "EXTENSION",
             "EFFECTIVE_FROM",
             "LOAD_DATETIME",
             "SOURCE"]
    }

    context.vault_structure_columns = {
        "MULTI_ACTIVE_SATELLITE": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATETIME",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
            "src_pk": "CUSTOMER_PK",
            "src_cdk": ["CUSTOMER_PHONE", "EXTENSION"],
            "src_payload": ["CUSTOMER_NAME"],
            "src_hashdiff": "HASHDIFF",
            "src_eff": "EFFECTIVE_FROM",
            "src_ldts": "LOAD_DATE",
            "src_source": "SOURCE"
        }
    }

    context.seed_config = {
        "RAW_STAGE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "RAW_STAGE_TS": {
<<<<<<< HEAD
            "+column_types": {
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
=======
            "column_types": {
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EFFECTIVE_FROM": "DATETIME2",
                "LOAD_DATETIME": "DATETIME2",
>>>>>>> dbtvault_update
                "SOURCE": "VARCHAR(50)"
            }
        },
        "RAW_STAGE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "RAW_STAGE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_ID": "DECIMAL(38, 0)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
<<<<<<< HEAD
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
=======
                "EFFECTIVE_FROM": "DATETIME2",
                "LOAD_DATETIME": "DATETIME2",
>>>>>>> dbtvault_update
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "CUSTOMER_NAME": "VARCHAR(50)",
<<<<<<< HEAD
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
=======
                "EFFECTIVE_FROM": "DATETIME2",
                "LOAD_DATETIME": "DATETIME2",
>>>>>>> dbtvault_update
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_TS": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "HASHDIFF": "BINARY(16)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
<<<<<<< HEAD
                "EFFECTIVE_FROM": "DATETIME",
                "LOAD_DATETIME": "DATETIME",
=======
                "EFFECTIVE_FROM": "DATETIME2",
                "LOAD_DATETIME": "DATETIME2",
>>>>>>> dbtvault_update
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        },
        "MULTI_ACTIVE_SATELLITE_TWO_CDK_NO_PK_CDK_HASHDIFF": {
<<<<<<< HEAD
            "+column_types": {
=======
            "column_types": {
>>>>>>> dbtvault_update
                "CUSTOMER_PK": "BINARY(16)",
                "CUSTOMER_NAME": "VARCHAR(50)",
                "CUSTOMER_PHONE": "VARCHAR(50)",
                "EXTENSION": "DECIMAL(38, 0)",
                "HASHDIFF": "BINARY(16)",
                "EFFECTIVE_FROM": "DATE",
                "LOAD_DATE": "DATE",
                "SOURCE": "VARCHAR(50)"
            }
        }
    }
