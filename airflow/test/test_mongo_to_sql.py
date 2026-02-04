from mongo_to_sql import to_int, normalize_delay_fields


def test_to_int_valid():
    assert to_int("12") == 12
    assert to_int(5) == 5


def test_to_int_invalid():
    assert to_int(None) is None
    assert to_int("abc") is None


def test_normalize_delay_fields_standard():
    doc = {
        "dep_delay": 10,
        "arr_delay": 5,
        "total_delay": 15,
    }
    dep, arr, total = normalize_delay_fields(doc)
    assert dep == 10
    assert arr == 5
    assert total == 15
