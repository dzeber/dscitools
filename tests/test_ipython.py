import pytest

from dscitools.ipython import (
    print_md,
    print_assertion,
    MarkdownMessage,
    StatusMessage,
    AssertMessage,
)

## Import the module itself so it can be monkeypatched.
from dscitools import ipython as ip
from tests.utils import call_func_in_notebook


@pytest.fixture
def markdown_text():
    return "Here is some __nice__ `Markdown`."


@pytest.fixture
def markdown_text_encoded():
    return "Here&nbsp;is&nbsp;some&nbsp;__nice__&nbsp;`Markdown`."


@pytest.fixture
def mock_now():
    return "Tue Nov 20 15:23:42 2018"


@pytest.fixture
def status_text():
    return "This happened"


@pytest.fixture
def assertion_result():
    return True


@pytest.fixture
def assertion_message():
    return "__Assert__  This happened:  `True`"


@pytest.fixture
def assertion_message_encoded():
    return "__Assert__&nbsp;&nbsp;This&nbsp;happened:&nbsp;&nbsp;`True`"


@pytest.fixture
def status_message():
    return "This happened:  Tue Nov 20 15:23:42 2018"


@pytest.fixture
def status_message_encoded():
    return (
        "__This&nbsp;happened:&nbsp;&nbsp;Tue&nbsp;Nov&nbsp;20&nbsp;"
        + "15:23:42&nbsp;2018__"
    )


def validate_notebook_output(output, expected_plain, expected_md):
    ## Did we get the appropriate rich output in the notebook?
    assert len(output) == 1
    output = output[0]
    assert output.get("output_type") == "display_data"
    data = output.get("data", {})
    assert data.get("text/plain") == expected_plain
    assert data.get("text/markdown") == expected_md


def test_print_md(markdown_text, markdown_text_encoded, capsys):
    ## Test in the console (non-rich) environment.
    print_md(markdown_text)
    output = capsys.readouterr().out
    assert output == markdown_text + "\n"

    ## Test in the notebook environment.
    nb_output = call_func_in_notebook(print_md, markdown_text)
    validate_notebook_output(nb_output, markdown_text, markdown_text_encoded)


def mock_print_status(status_text, mock_now_str):
    ## Mock the current time manually rather than using pytest's monkeypatch
    ## so as to work with the run_in_notebook() function.
    # TODO: still necessary?
    def mocknow():
        return mock_now_str

    from dscitools import ipython as ip

    _oldnow = ip.now
    ip.now = mocknow
    ip.print_status(status_text)
    ip.now = _oldnow


def test_print_status(
    status_text, status_message, status_message_encoded, capsys, mock_now
):
    ## Test in the console (non-rich) environment.
    mock_print_status(status_text, mock_now)
    output = capsys.readouterr().out
    assert output == status_message + "\n"

    ## Test in the notebook environment.
    nb_output = call_func_in_notebook(mock_print_status, status_text, mock_now)
    validate_notebook_output(nb_output, status_message, status_message_encoded)


def test_print_assertion(
    status_text,
    assertion_result,
    assertion_message,
    assertion_message_encoded,
    capsys,
):
    ## Test in the console (non-rich) environment.
    print_assertion(status_text, assertion_result)
    output = capsys.readouterr().out
    assert output == assertion_message + "\n"

    ## Test in the notebook environment.
    nb_output = call_func_in_notebook(
        print_assertion, status_text, assertion_result
    )
    validate_notebook_output(
        nb_output, assertion_message, assertion_message_encoded
    )


def test_markdownmessage(markdown_text, markdown_text_encoded):
    md_obj = MarkdownMessage(markdown_text)
    assert md_obj.__repr__() == markdown_text
    assert md_obj._repr_markdown_() == markdown_text_encoded


def test_statusmessage(
    status_text, status_message, status_message_encoded, monkeypatch, mock_now
):
    def mocknow():
        return mock_now

    monkeypatch.setattr(ip, "now", mocknow)

    md_obj = StatusMessage(status_text)
    assert md_obj.__repr__() == status_message
    assert md_obj._repr_markdown_() == status_message_encoded


def test_assertmessage(
    status_text, assertion_result, assertion_message, assertion_message_encoded
):
    md_obj = AssertMessage(status_text, assertion_result)
    assert md_obj.__repr__() == assertion_message
    assert md_obj._repr_markdown_() == assertion_message_encoded
