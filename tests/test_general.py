import pytest
from numpy import array as nparray
from dscitools.general import fmt_count


@pytest.fixture
def count():
    return 3592


@pytest.fixture
def description():
    return "Number of items"


@pytest.fixture
def overall_total():
    return 10000


@pytest.fixture
def overall_description():
    return "overall"


@pytest.fixture
def np_data(count, overall_total):
    return nparray([count, overall_total - count],
                   dtype="int64")


@pytest.fixture
def float_count():
    return 27.82


@pytest.fixture
def float_overall_total():
    return 146.3


def test_fmt_count_basic(count):
    assert (
        fmt_count(count,
                  print_result=False) ==
        "3,592"
    )


def test_fmt_count_with_description(count, description):
    assert (
        fmt_count(count,
                  description,
                  print_result=False) ==
        "Number of items:  3,592"
    )


def test_fmt_count_with_total(count, description, overall_total):
    assert (
        fmt_count(count,
                  description,
                  overall_total,
                  print_result=False) ==
        "Number of items:  3,592 out of 10,000  (35.92%)"
    )


def test_fmt_count_with_total_description(count,
                                          description,
                                          overall_total,
                                          overall_description):
    assert (
        fmt_count(count,
                  description,
                  overall_total,
                  overall_description,
                  print_result=False) ==
        "Number of items:  3,592 out of 10,000 overall  (35.92%)"
    )


def test_fmt_count_hide_total(count, description, overall_total):
    assert (
        fmt_count(count,
                  description,
                  overall_total,
                  show_n_overall=False,
                  print_result=False) ==
        "Number of items:  3,592  (35.92%)"
    )


def test_fmt_count_hide_total_with_description(count,
                                               description,
                                               overall_total,
                                               overall_description):
    assert (
        fmt_count(count,
                  description,
                  overall_total,
                  overall_description,
                  show_n_overall=False,
                  print_result=False) ==
        "Number of items:  3,592  (35.92% of overall)"
    )


def test_fmt_count_print_result(count, description, capsys):
    fmt_count(count,
              description,
              print_result=True)
    output = capsys.readouterr()
    assert output.out == "Number of items:  3,592\n"


def test_fmt_count_with_np_types(np_data, description):
    count = np_data[0]
    total = np_data.sum()
    assert (
        fmt_count(count,
                  description,
                  total,
                  print_result=False) ==
        "Number of items:  3,592 out of 10,000  (35.92%)"
    )


def test_fmt_count_with_zero_total(count, description):
    ## This falls back silently to the case where 'n_overall' is not given.
    assert (
        fmt_count(count,
                  description,
                  n_overall=0,
                  print_result=False) ==
        "Number of items:  3,592"
    )


def test_fmt_count_with_floats(float_count, description, float_overall_total):
    assert (
        fmt_count(float_count,
                  description,
                  float_overall_total,
                  print_result=False) ==
        "Number of items:  27.82 out of 146.3  (19.02%)"
    )
