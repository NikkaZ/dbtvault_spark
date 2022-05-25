import os
from unittest.mock import patch

<<<<<<< HEAD
import dbtvault_harness_utils


@patch.dict(os.environ, {'CIRCLE_NODE_INDEX': '0', 'CIRCLE_JOB': 'test_job', 'CIRCLE_BRANCH': 'test'})
def test_is_pipeline_true():
    assert dbtvault_harness_utils.is_pipeline()


@patch.dict(os.environ, {'CIRCLE_NODE_INDEX': '0'})
def test_is_pipeline_false_some_environment():
    assert not dbtvault_harness_utils.is_pipeline()


def test_is_pipeline_false_no_environment():
    assert not dbtvault_harness_utils.is_pipeline()
=======
from env import env_utils
from test import dbtvault_harness_utils


@patch.dict(os.environ, {'PIPELINE_JOB': 'test_job', 'PIPELINE_BRANCH': 'test'}, clear=True)
def test_is_pipeline_true():
    assert env_utils.is_pipeline()


@patch.dict(os.environ, {'PIPELINE_JOB': '0'}, clear=True)
def test_is_pipeline_false_some_environment():
    assert not env_utils.is_pipeline()


def test_is_pipeline_false_no_environment():
    assert not env_utils.is_pipeline()
>>>>>>> dbtvault_update
