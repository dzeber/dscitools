import pytest
from numpy import array as nparray

from dscitools import fmt_count, now, today

COUNT = 3592
DESCRIPTION = "Number of items"
OVERALL_TOTAL = 10000
OVERALL_DESCRIPTION = "overall"
FLOAT_COUNT = 27.82
FLOAT_OVERALL_TOTAL = 146.3


@pytest.fixture
def np_data():
    return nparray([COUNT, OVERALL_TOTAL - COUNT], dtype="int64")


def test_now(mock_general_datetime):
    assert now() == "Sat Apr 18 12:36:10 2020"


def test_today(mock_general_datetime):
    assert today() == "2020-04-18"


def test_fmt_count_basic(capsys):
    fmtted = fmt_count(COUNT, print_result=False)
    assert fmtted == "3,592"

    fmtted = fmt_count(COUNT, DESCRIPTION, print_result=False)
    assert fmtted == "Number of items:  3,592"

    fmtted = fmt_count(COUNT, DESCRIPTION, OVERALL_TOTAL, print_result=False)
    assert fmtted == "Number of items:  3,592 out of 10,000  (35.92%)"

    fmtted = fmt_count(
        COUNT,
        DESCRIPTION,
        OVERALL_TOTAL,
        OVERALL_DESCRIPTION,
        print_result=False,
    )
    assert fmtted == "Number of items:  3,592 out of 10,000 overall  (35.92%)"

    fmt_count(COUNT, DESCRIPTION, print_result=True)
    output = capsys.readouterr()
    assert output.out == "Number of items:  3,592\n"


def test_fmt_count_hide_total():
    fmtted = fmt_count(
        COUNT,
        DESCRIPTION,
        OVERALL_TOTAL,
        show_n_overall=False,
        print_result=False,
    )
    assert fmtted == "Number of items:  3,592  (35.92%)"

    fmtted = fmt_count(
        COUNT,
        DESCRIPTION,
        OVERALL_TOTAL,
        OVERALL_DESCRIPTION,
        show_n_overall=False,
        print_result=False,
    )
    assert fmtted == "Number of items:  3,592  (35.92% of overall)"


def test_fmt_count_with_np_types(np_data):
    count = np_data[0]
    total = np_data.sum()
    fmtted = fmt_count(count, DESCRIPTION, total, print_result=False)
    assert fmtted == "Number of items:  3,592 out of 10,000  (35.92%)"


def test_fmt_count_with_zero_total():
    # This falls back silently to the case where 'n_overall' is not given.
    fmtted = fmt_count(COUNT, DESCRIPTION, n_overall=0, print_result=False)
    assert fmtted == "Number of items:  3,592"


def test_fmt_count_with_floats():
    fmtted = fmt_count(
        FLOAT_COUNT, DESCRIPTION, FLOAT_OVERALL_TOTAL, print_result=False
    )
    assert fmtted == "Number of items:  27.82 out of 146.3  (19.02%)"
