
import pytest
from dscitools.general import show_count


### TODO:
### - testing for time and date functions

@pytest.fixture
def count():
    return 3592


@pytest.fixture
def description():
    return "number of items"


@pytest.fixture
def overall_total():
    return 10000


@pytest.fixture
def overall_description():
    return "overall"


def test_show_basic_count(count):
    assert (
        show_count(count,
                   print_result=False) ==
        "3,592"
    )


def test_show_count_with_description(count, description):
    assert (
        show_count(count,
                   description,
                   print_result = False) ==
        "Number of items:  3,592"
    )


def test_show_count_with_total(count, description, overall_total):
    assert (
        show_count(count,
                   description,
                   overall_total,
                   print_result=False) ==
        "Number of items:  3,592 out of 10,000  (35.92%)"
    )


def test_show_count_with_total_description(count,
                                           description,
                                           overall_total,
                                           overall_description):
    assert (
        show_count(count,
                   description,
                   overall_total,
                   overall_description,
                   print_result=False) ==
        "Number of items:  3,592 out of 10,000 overall  (35.92%)"
    )


def test_show_count_hide_total(count, description, overall_total):
    assert (
        show_count(count,
                   description,
                   overall_total,
                   show_n_overall=False,
                   print_result=False) ==
        "Number of items:  3,592  (35.92%)"
    )


def test_show_count_hide_total_with_description(count,
                                                description,
                                                overall_total,
                                                overall_description):
    assert (
        show_count(count,
                   description,
                   overall_total,
                   overall_description,
                   show_n_overall=False,
                   print_result=False) ==
        "Number of items:  3,592  (35.92% of overall)"
    )


