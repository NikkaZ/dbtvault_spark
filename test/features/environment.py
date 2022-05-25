from behave.fixture import use_fixture_by_tag

<<<<<<< HEAD
from test import dbtvault_generator
from test import dbtvault_harness_utils
import test
from test.features.behave_fixtures import *
=======
from env import env_utils
from test import dbtvault_generator
from test import dbtvault_harness_utils
from test.features import behave_fixtures
>>>>>>> dbtvault_update
from test.features.bridge import fixtures_bridge
from test.features.cycle import fixtures_cycle
from test.features.eff_sats import fixtures_eff_sat
from test.features.hubs import fixtures_hub
from test.features.links import fixtures_link
from test.features.ma_sats import fixtures_ma_sat
<<<<<<< HEAD
from test.features.sats_with_oos import fixtures_oos_sat
=======
>>>>>>> dbtvault_update
from test.features.pit import fixtures_pit
from test.features.sats import fixtures_sat
from test.features.staging import fixtures_staging
from test.features.t_links import fixtures_t_link
from test.features.xts import fixtures_xts

fixture_registry_utils = {
<<<<<<< HEAD
    "fixture.enable_sha": enable_sha,
    "fixture.enable_auto_end_date": enable_auto_end_date,
    "fixture.enable_full_refresh": enable_full_refresh,
    "fixture.disable_union": disable_union,
    "fixture.disable_payload": disable_payload
=======
    "fixture.enable_sha": behave_fixtures.enable_sha,
    "fixture.enable_auto_end_date": behave_fixtures.enable_auto_end_date,
    "fixture.enable_full_refresh": behave_fixtures.enable_full_refresh,
    "fixture.disable_union": behave_fixtures.disable_union,
    "fixture.disable_payload": behave_fixtures.disable_payload
>>>>>>> dbtvault_update
}

fixtures_registry = {
    "fixture.staging":
<<<<<<< HEAD
        {"snowflake": fixtures_staging.staging,
         "bigquery": fixtures_staging.staging_bigquery,
         "sqlserver": fixtures_staging.staging_sqlserver},

    "fixture.single_source_hub":
        {"snowflake": fixtures_hub.single_source_hub,
         "bigquery": fixtures_hub.single_source_hub_bigquery,
         "sqlserver": fixtures_hub.single_source_hub_sqlserver},

    "fixture.multi_source_hub":
        {"snowflake": fixtures_hub.multi_source_hub,
         "bigquery": fixtures_hub.multi_source_hub_bigquery,
         "sqlserver": fixtures_hub.multi_source_hub_sqlserver},

    "fixture.single_source_link":
        {"snowflake": fixtures_link.single_source_link,
         "bigquery": fixtures_link.single_source_link_bigquery,
         "sqlserver": fixtures_link.single_source_link_sqlserver},

    "fixture.multi_source_link":
        {"snowflake": fixtures_link.multi_source_link,
         "bigquery": fixtures_link.multi_source_link_bigquery,
         "sqlserver": fixtures_link.multi_source_link_sqlserver},
=======
        {"snowflake": fixtures_staging.staging_snowflake,
         "bigquery": fixtures_staging.staging_bigquery,
         "sqlserver": fixtures_staging.staging_sqlserver,
         "databricks": ''},

    "fixture.staging_escaped":
        {"snowflake": fixtures_staging.staging_escaped_snowflake,
         "bigquery": fixtures_staging.staging_escaped_bigquery,
         "sqlserver": fixtures_staging.staging_escaped_sqlserver,
         "databricks": ''},

    "fixture.single_source_hub":
        {"snowflake": fixtures_hub.single_source_hub_snowflake,
         "bigquery": fixtures_hub.single_source_hub_bigquery,
         "sqlserver": fixtures_hub.single_source_hub_sqlserver,
         "databricks": fixtures_hub.single_source_hub_databricks},

    "fixture.single_source_comppk_hub":
        {"snowflake": fixtures_hub.single_source_comppk_hub_snowflake,
         "bigquery": fixtures_hub.single_source_comppk_hub_bigquery,
         "sqlserver": fixtures_hub.single_source_comppk_hub_sqlserver,
         "databricks": ''},

    "fixture.single_source_comppknk_hub":
        {"snowflake": fixtures_hub.single_source_comppknk_hub_snowflake,
         "bigquery": fixtures_hub.single_source_comppknk_hub_bigquery,
         "sqlserver": fixtures_hub.single_source_comppknk_hub_sqlserver,
         "databricks": ''},

    "fixture.multi_source_hub":
        {"snowflake": fixtures_hub.multi_source_hub_snowflake,
         "bigquery": fixtures_hub.multi_source_hub_bigquery,
         "sqlserver": fixtures_hub.multi_source_hub_sqlserver,
         "databricks": fixtures_hub.multi_source_hub_databricks},

    "fixture.multi_source_comppk_hub":
        {"snowflake": fixtures_hub.multi_source_comppk_hub_snowflake,
         "bigquery": fixtures_hub.multi_source_comppk_hub_bigquery,
         "sqlserver": fixtures_hub.multi_source_comppk_hub_sqlserver,
         "databricks": ''},

    "fixture.single_source_link":
        {"snowflake": fixtures_link.single_source_link_snowflake,
         "bigquery": fixtures_link.single_source_link_bigquery,
         "sqlserver": fixtures_link.single_source_link_sqlserver,
         "databricks": fixtures_link.single_source_link_databricks},

    "fixture.single_source_comppk_link":
        {"snowflake": fixtures_link.single_source_comppk_link_snowflake,
         "bigquery": fixtures_link.single_source_comppk_link_bigquery,
         "sqlserver": fixtures_link.single_source_comppk_link_sqlserver,
         "databricks": ''},

    "fixture.multi_source_link":
        {"snowflake": fixtures_link.multi_source_link_snowflake,
         "bigquery": fixtures_link.multi_source_link_bigquery,
         "sqlserver": fixtures_link.multi_source_link_sqlserver,
         "databricks": ''},
>>>>>>> dbtvault_update

    "fixture.t_link":
        {"snowflake": fixtures_t_link.t_link,
         "bigquery": fixtures_t_link.t_link_bigquery,
<<<<<<< HEAD
         "sqlserver": fixtures_t_link.t_link_sqlserver},

    "fixture.satellite":
        {"snowflake": fixtures_sat.satellite,
         "bigquery": fixtures_sat.satellite_bigquery,
         "sqlserver": fixtures_sat.satellite_sqlserver},

    "fixture.satellite_cycle":
        {"snowflake": fixtures_sat.satellite_cycle,
         "bigquery": fixtures_sat.satellite_cycle_bigquery,
         "sqlserver": fixtures_sat.satellite_cycle_sqlserver},

    "fixture.eff_satellite":
        {"snowflake": fixtures_eff_sat.eff_satellite,
         "bigquery": fixtures_eff_sat.eff_satellite_bigquery,
         "sqlserver": fixtures_eff_sat.eff_satellite_sqlserver},

    "fixture.eff_satellite_testing_auto_end_dating":
        {"snowflake": fixtures_eff_sat.eff_satellite_testing_auto_end_dating,
         "bigquery": fixtures_eff_sat.eff_satellite_testing_auto_end_dating_bigquery,
         "sqlserver": fixtures_eff_sat.eff_satellite_testing_auto_end_dating_sqlserver},

    "fixture.eff_satellite_multipart":
        {"snowflake": fixtures_eff_sat.eff_satellite_multipart,
         "bigquery": fixtures_eff_sat.eff_satellite_multipart_bigquery,
         "sqlserver": fixtures_eff_sat.eff_satellite_multipart_sqlserver},

    "fixture.out_of_sequence_satellite":
        {"snowflake": fixtures_oos_sat.out_of_sequence_satellite,
         "bigquery": "",
         "sqlserver": fixtures_oos_sat.out_of_sequence_satellite_sqlserver},

    "fixture.multi_active_satellite":
        {"snowflake": fixtures_ma_sat.multi_active_satellite,
         "bigquery": fixtures_ma_sat.multi_active_satellite_bigquery,
         "sqlserver": fixtures_ma_sat.multi_active_satellite_sqlserver},

    "fixture.multi_active_satellite_cycle":
        {"snowflake": fixtures_ma_sat.multi_active_satellite_cycle,
         "bigquery": fixtures_ma_sat.multi_active_satellite_cycle_bigquery,
         "sqlserver": fixtures_ma_sat.multi_active_satellite_cycle_sqlserver},

    "fixture.xts":
        {"snowflake": fixtures_xts.xts,
         "bigquery": fixtures_xts.xts_bigquery,
         "sqlserver": fixtures_xts.xts_sqlserver},

    "fixture.pit":
        {"snowflake": fixtures_pit.pit,
         "bigquery": fixtures_pit.pit_bigquery,
         "sqlserver": fixtures_pit.pit_sqlserver},

    "fixture.pit_one_sat":
        {"snowflake": fixtures_pit.pit_one_sat,
         "bigquery": fixtures_pit.pit_one_sat_bigquery,
         "sqlserver": fixtures_pit.pit_one_sat_sqlserver},

    "fixture.pit_two_sats":
        {"snowflake": fixtures_pit.pit_two_sats,
         "bigquery": fixtures_pit.pit_two_sats_bigquery,
         "sqlserver": fixtures_pit.pit_two_sats_sqlserver},

    "fixture.bridge":
        {"snowflake": fixtures_bridge.bridge,
         "bigquery": fixtures_bridge.bridge_bigquery,
         "sqlserver": fixtures_bridge.bridge_sqlserver},

    "fixture.cycle":
        {"snowflake": fixtures_cycle.cycle,
         "bigquery": fixtures_cycle.cycle_bigquery,
         "sqlserver": fixtures_cycle.cycle_sqlserver},
=======
         "sqlserver": fixtures_t_link.t_link_sqlserver,
         "databricks": ''},

    "fixture.t_link_comppk":
        {"snowflake": fixtures_t_link.t_link_comppk,
         "bigquery": fixtures_t_link.t_link_comppk_bigquery,
         "sqlserver": fixtures_t_link.t_link_comppk_sqlserver,
         "databricks": ''},

    "fixture.satellite":
        {"snowflake": fixtures_sat.satellite_snowflake,
         "bigquery": fixtures_sat.satellite_bigquery,
         "sqlserver": fixtures_sat.satellite_sqlserver,
         "databricks": ''},

    "fixture.satellite_cycle":
        {"snowflake": fixtures_sat.satellite_cycle_snowflake,
         "bigquery": fixtures_sat.satellite_cycle_bigquery,
         "sqlserver": fixtures_sat.satellite_cycle_sqlserver,
         "databricks": ''},

    "fixture.eff_satellite":
        {"snowflake": fixtures_eff_sat.eff_satellite_snowflake,
         "bigquery": fixtures_eff_sat.eff_satellite_bigquery,
         "sqlserver": fixtures_eff_sat.eff_satellite_sqlserver,
         "databricks": ''},

    "fixture.eff_satellite_datetime":
        {"snowflake": fixtures_eff_sat.eff_satellite_datetime_snowflake,
         "bigquery": fixtures_eff_sat.eff_satellite_datetime_bigquery,
         "sqlserver": fixtures_eff_sat.eff_satellite_datetime_sqlserver,
         "databricks": ''},

    "fixture.eff_satellite_testing_auto_end_dating":
        {"snowflake": fixtures_eff_sat.eff_satellite_testing_auto_end_dating_snowflake,
         "bigquery": fixtures_eff_sat.eff_satellite_testing_auto_end_dating_bigquery,
         "sqlserver": fixtures_eff_sat.eff_satellite_testing_auto_end_dating_sqlserver,
         "databricks": ''},

    "fixture.eff_satellite_multipart":
        {"snowflake": fixtures_eff_sat.eff_satellite_multipart_snowflake,
         "bigquery": fixtures_eff_sat.eff_satellite_multipart_bigquery,
         "sqlserver": fixtures_eff_sat.eff_satellite_multipart_sqlserver,
         "databricks": ''},

    "fixture.multi_active_satellite":
        {"snowflake": fixtures_ma_sat.multi_active_satellite_snowflake,
         "bigquery": fixtures_ma_sat.multi_active_satellite_bigquery,
         "sqlserver": fixtures_ma_sat.multi_active_satellite_sqlserver,
         "databricks": ''},

    "fixture.multi_active_satellite_cycle":
        {"snowflake": fixtures_ma_sat.multi_active_satellite_cycle_snowflake,
         "bigquery": fixtures_ma_sat.multi_active_satellite_cycle_bigquery,
         "sqlserver": fixtures_ma_sat.multi_active_satellite_cycle_sqlserver,
         "databricks": ''},

    "fixture.xts":
        {"snowflake": fixtures_xts.xts_snowflake,
         "bigquery": fixtures_xts.xts_bigquery,
         "sqlserver": fixtures_xts.xts_sqlserver,
         "databricks": ''},

    "fixture.pit":
        {"snowflake": fixtures_pit.pit_snowflake,
         "bigquery": fixtures_pit.pit_bigquery,
         "sqlserver": fixtures_pit.pit_sqlserver,
         "databricks": ''},

    "fixture.pit_one_sat":
        {"snowflake": fixtures_pit.pit_one_sat_snowflake,
         "bigquery": fixtures_pit.pit_one_sat_bigquery,
         "sqlserver": fixtures_pit.pit_one_sat_sqlserver,
         "databricks": ''},

    "fixture.pit_two_sats":
        {"snowflake": fixtures_pit.pit_two_sats_snowflake,
         "bigquery": fixtures_pit.pit_two_sats_bigquery,
         "sqlserver": fixtures_pit.pit_two_sats_sqlserver,
         "databricks": ''},

    "fixture.bridge":
        {"snowflake": fixtures_bridge.bridge_snowflake,
         "bigquery": fixtures_bridge.bridge_bigquery,
         "sqlserver": fixtures_bridge.bridge_sqlserver,
         "databricks": ''},

    "fixture.cycle":
        {"snowflake": fixtures_cycle.cycle_snowflake,
         "bigquery": fixtures_cycle.cycle_bigquery,
         "sqlserver": fixtures_cycle.cycle_sqlserver,
         "databricks": ''},
>>>>>>> dbtvault_update

}

fixture_registry_snowflake = {k: v['snowflake'] for k, v in fixtures_registry.items()}
fixture_registry_bigquery = {k: v['bigquery'] for k, v in fixtures_registry.items()}
fixture_registry_sqlserver = {k: v['sqlserver'] for k, v in fixtures_registry.items()}
<<<<<<< HEAD
=======
fixture_registry_databricks = {k: v['databricks'] for k, v in fixtures_registry.items()}
>>>>>>> dbtvault_update

fixture_lookup = {
    'snowflake': fixture_registry_utils | fixture_registry_snowflake,
    'bigquery': fixture_registry_utils | fixture_registry_bigquery,
<<<<<<< HEAD
    'sqlserver': fixture_registry_utils | fixture_registry_sqlserver
=======
    'sqlserver': fixture_registry_utils | fixture_registry_sqlserver,
    'databricks': fixture_registry_utils | fixture_registry_databricks,
>>>>>>> dbtvault_update
}


def before_all(context):
    """
    Set up the full test environment and add objects to the context for use in steps
    """

    # Setup context
    context.config.setup_logging()

    # Env setup
<<<<<<< HEAD
    dbtvault_harness_utils.setup_environment()

    # Restore modified YAML to starting state
=======
    env_utils.setup_environment()

    # Delete temp YAML files
>>>>>>> dbtvault_update
    dbtvault_generator.clean_test_schema_file()

    # Backup YAML prior to run
    dbtvault_generator.backup_project_yml()


def after_all(context):
    """
    Force Restore of dbt_project.yml
    """

    dbtvault_generator.restore_project_yml()


<<<<<<< HEAD
def before_scenario(context, scenario):
    dbtvault_harness_utils.create_dummy_model()
    dbtvault_harness_utils.replace_test_schema()

    dbtvault_harness_utils.clean_csv()
    dbtvault_harness_utils.clean_models()
    dbtvault_harness_utils.clean_target()

    dbtvault_generator.clean_test_schema_file()


def before_tag(context, tag):
    tgt = dbtvault_harness_utils.platform()

    if tgt in test.AVAILABLE_PLATFORMS:
=======
def decide_to_run(tags, obj, obj_type):
    platforms = set(env_utils.AVAILABLE_PLATFORMS)
    obj_tags = set([t.lower() for t in tags])
    valid_tags = list(platforms.intersection(obj_tags))
    valid_tags_with_not = {f"not_{plt}" for plt in platforms}.intersection(obj_tags)

    # Find platforms which have the platform name x from each string not_x
    do_not_run_on = [s for s in platforms if any(s in tag for tag in valid_tags_with_not)]

    if not env_utils.platform() in valid_tags:
        if len(valid_tags) > 0:
            obj.skip(
                f"{obj_type} skipped. This {obj_type} will only run on {', '.join([t.upper() for t in valid_tags])}")
            return False

    if len(do_not_run_on) > 0:

        only_run_on = set(platforms).difference(set(do_not_run_on))

        if env_utils.platform() in do_not_run_on:
            obj.skip(
                f"{obj_type} skipped. This {obj_type} will only run on {', '.join(only_run_on)}")
            return False

    return True


def before_feature(context, feature):
    decide_to_run(feature.tags, feature, 'Feature')


def before_scenario(context, scenario):
    do_run = decide_to_run(scenario.effective_tags, scenario, 'Scenario')

    if do_run:
        dbtvault_harness_utils.create_dummy_model()
        dbtvault_harness_utils.replace_test_schema()

        dbtvault_harness_utils.clean_seeds()
        dbtvault_harness_utils.clean_models()
        dbtvault_harness_utils.clean_target()

        dbtvault_generator.clean_test_schema_file()


def before_tag(context, tag):
    tgt = env_utils.platform()

    if tgt in env_utils.AVAILABLE_PLATFORMS:
>>>>>>> dbtvault_update
        fixtures = fixture_lookup[tgt]
        if tag.startswith("fixture."):
            return use_fixture_by_tag(tag, context, fixtures)
    else:
<<<<<<< HEAD
        raise ValueError(f"Target must be set to one of: {', '.join(test.AVAILABLE_PLATFORMS)}")
=======
        raise ValueError(f"Target must be set to one of: {', '.join(env_utils.AVAILABLE_PLATFORMS)}")
>>>>>>> dbtvault_update
