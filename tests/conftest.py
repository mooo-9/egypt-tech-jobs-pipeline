import pytest

from pipeline.extract import common


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Tests never wait on retries or rate limiting."""
    common._last_call.clear()
    common.TRUNCATED.clear()
    monkeypatch.setattr(common.time, "sleep", lambda s: None)
