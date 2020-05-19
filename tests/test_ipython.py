from dscitools.ipython import (
    print_md,
    print_status,
    print_assertion,
    MarkdownMessage,
    StatusMessage,
    AssertMessage,
)
from tests.utils import get_notebook_rich_output

MARKDOWN_TEXT = "Here is some __nice__ `Markdown`."
STATUS_TEXT = "This happened"
ASSERT_RESULT = True


def encode_message(message):
    """Encoded message as returned in notebook rich Markdown output."""
    return message.replace(" ", "&nbsp;")


def validate_console_output(capsys, result, func, *args):
    """Test in the console (non-rich) environment."""
    func(*args)
    output = capsys.readouterr().out
    assert output == result + "\n"


def validate_notebook_output(result, result_md, func, *args):
    """Test rich output in the notebook environment."""
    if result_md is None:
        result_md = encode_message(result)
    output = get_notebook_rich_output(func, *args)
    assert output.get("text/plain") == result
    assert output.get("text/markdown") == result_md


def test_print_md(capsys):
    expected = "Here is some __nice__ `Markdown`."
    expected_md = encode_message(expected)

    validate_console_output(capsys, expected, print_md, MARKDOWN_TEXT)
    validate_notebook_output(expected, expected_md, print_md, MARKDOWN_TEXT)


def mock_print_status(status_text, mock_dt):
    # Mock the current time manually rather than using pytest's monkeypatch
    # so as to work with the `call_func_in_notebook()` function.
    from dscitools import general

    general.datetime.datetime = mock_dt
    print_status(status_text)


def test_print_status(capsys, mock_dt):
    expected = "This happened:  Sat Apr 18 12:36:10 2020"
    expected_md = encode_message(
        "__This happened:  Sat Apr 18 12:36:10 2020__"
    )

    validate_console_output(
        capsys, expected, mock_print_status, STATUS_TEXT, mock_dt
    )
    validate_notebook_output(
        expected, expected_md, mock_print_status, STATUS_TEXT, mock_dt
    )


def test_print_assertion(capsys):
    expected = "__Assert__  This happened:  `True`"
    expected_md = encode_message(expected)

    validate_console_output(
        capsys, expected, print_assertion, STATUS_TEXT, ASSERT_RESULT
    )
    validate_notebook_output(
        expected, expected_md, print_assertion, STATUS_TEXT, ASSERT_RESULT
    )


def test_markdownmessage():
    expected = "Here is some __nice__ `Markdown`."
    expected_md = encode_message(expected)

    md_obj = MarkdownMessage(MARKDOWN_TEXT)
    assert md_obj.__repr__() == expected
    assert md_obj._repr_markdown_() == expected_md


def test_statusmessage(mock_general_datetime):
    expected = "This happened:  Sat Apr 18 12:36:10 2020"
    expected_md = encode_message(
        "__This happened:  Sat Apr 18 12:36:10 2020__"
    )

    md_obj = StatusMessage(STATUS_TEXT)
    assert md_obj.__repr__() == expected
    assert md_obj._repr_markdown_() == expected_md


def test_assertmessage():
    expected = "__Assert__  This happened:  `True`"
    expected_md = encode_message(expected)

    md_obj = AssertMessage(STATUS_TEXT, ASSERT_RESULT)
    assert md_obj.__repr__() == expected
    assert md_obj._repr_markdown_() == expected_md
