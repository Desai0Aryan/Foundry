import pytest

from foundry.config import Settings


def test_heartbeat_must_fit_in_lease():
    with pytest.raises(ValueError, match="heartbeat_seconds"):
        Settings(lease_seconds=10, heartbeat_seconds=5)
    Settings(lease_seconds=10, heartbeat_seconds=4.9)


def test_production_refuses_default_secrets():
    with pytest.raises(ValueError, match="default dev secrets"):
        Settings(environment="production")
