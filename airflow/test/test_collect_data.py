import os

os.environ["AIRLABS_API_KEY"] = "DUMMY_FOR_TESTS"

import pytest

from Collect_data import is_quota_error

def test_is_quota_error_true():
    data = {"error": {"code": "month_limit_exceeded"}}
    assert is_quota_error(data) is True


def test_is_quota_error_false():
    data = {"response": []}
    assert is_quota_error(data) is False
