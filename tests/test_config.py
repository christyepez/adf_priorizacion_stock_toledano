import pytest

from priorizacion_stock_toledano.config import validate_control_mode


def test_validate_control_mode_accepts_get_control_cargas():
    assert validate_control_mode("get_control_cargas") == "get_control_cargas"


def test_validate_control_mode_accepts_lakebase():
    assert validate_control_mode("lakebase") == "lakebase"


def test_validate_control_mode_rejects_unknown_value():
    with pytest.raises(ValueError):
        validate_control_mode("otro")
