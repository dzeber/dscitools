import pytest

from dscitools.general import datetime

MOCK_DATETIME = datetime.datetime(2020, 4, 18, 12, 36, 10)


# Mock datetime and date to return a specific date.
class MockDT(datetime.datetime):
    @classmethod
    def now(cls):
        return MOCK_DATETIME


class MockDate(datetime.date):
    @classmethod
    def today(cls):
        return MOCK_DATETIME.date()


@pytest.fixture
def mock_general_datetime(monkeypatch):
    monkeypatch.setattr(datetime, "datetime", MockDT)
    monkeypatch.setattr(datetime, "date", MockDate)


@pytest.fixture
def mock_dt():
    return MockDT
