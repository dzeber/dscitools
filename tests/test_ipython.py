import pytest
import ast
from nbformat.v4 import new_notebook, new_code_cell
from nbconvert.preprocessors import ExecutePreprocessor
from dscitools.ipython import (
    print_md,
    print_assertion,
    MarkdownMessage,
    StatusMessage,
    AssertMessage
)
## Import the module itself so it can be monkeypatched.
from dscitools import ipython as ip


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
    return "__This&nbsp;happened:&nbsp;&nbsp;Tue&nbsp;Nov&nbsp;20&nbsp;" + \
           "15:23:42&nbsp;2018__"


def run_in_notebook(func_name, *args):
    """Call a function in a Jupyter notebook code cell and capture the output.

    This can be used for testing code that interacts with the notebook
    environment, eg. rich output formatting. For clarity and consistency with
    `pytest` philosophy, the code to be run should be encapsulated in a single
    function defined in this module which accepts simple args (see below).

    Parameters
    ----------
    func_name : str
        The name of the function to run. It must be importable from this module.
    args: str
        Arguments to pass to the function. These must be either Python literal
        strings or names of symbols defined in this module. If a symbol is
        callable, it will be replaced with its return value prior to calling
        the function.

    Returns
    -------
    dict
        The dict representing the cell output in the Jupyter notebook format.
    """
    ## Symbols that need to be imported in order to run the notebook.
    to_import = [func_name]
    ## String representations of the args to pass to the function.
    call_args = []
    for a in args:
        try:
            ## Is the arg a valid string literal?
            ast.literal_eval(a)
        except ValueError:
            ## Arg is not a literal.
            ## Interpret as a symbol name.
            if a not in globals():
                err_msg = "Notebook test runner can only handle function" + \
                          " args that are either literal or defined symbol" + \
                          " names"
                raise ValueError(err_msg.format(a))
            to_import.append(a)
            if callable(globals()[a]):
                ## The arg represents a function.
                ## Make sure to evaluate it first.
                a += "()"
        finally:
            call_args.append(a)

    to_import_str = ",".join(to_import)
    call_args_str = ",".join(call_args)
    source = "from {module} import {symb}".format(module=__name__,
                                                  symb=to_import_str)
    source += "\n{func}({args})".format(func=func_name,
                                        args=call_args_str)

    ## Create a notebook with a single code cell that calls the function.
    code_cell = new_code_cell(source)
    nb = new_notebook()
    nb.cells.append(code_cell)
    ## Run the notebook to capture the output.
    ep = ExecutePreprocessor()
    ep.preprocess(nb, {})

    return code_cell.outputs


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
    ## Enclose the message text in additional quotes so it is interpreted as
    ## a string literal by run_in_notebook().
    nb_output = run_in_notebook("print_md", "'{}'".format(markdown_text))
    validate_notebook_output(nb_output, markdown_text, markdown_text_encoded)


def mock_print_status(status_text, mock_now_str):
    ## Mock the current time manually rather than using pytest's monkeypatch
    ## so as to work with the run_in_notebook() function.
    def mocknow():
        return mock_now_str
    from dscitools import ipython as ip
    _oldnow = ip.now
    ip.now = mocknow
    ip.print_status(status_text)
    ip.now = _oldnow


def test_print_status(status_text,
                      status_message,
                      status_message_encoded,
                      capsys,
                      mock_now):
    ## Test in the console (non-rich) environment.
    mock_print_status(status_text, mock_now)
    output = capsys.readouterr().out
    assert output == status_message + "\n"

    ## Test in the notebook environment.
    ## Enclose the message text in additional text so it is interpreted as
    ## a string literal by run_in_notebook().
    nb_output = run_in_notebook("mock_print_status",
                                "'{}'".format(status_text),
                                "'{}'".format(mock_now))
    validate_notebook_output(nb_output, status_message, status_message_encoded)


def test_print_assertion(status_text,
                         assertion_result,
                         assertion_message,
                         assertion_message_encoded,
                         capsys):
    ## Test in the console (non-rich) environment.
    print_assertion(status_text, assertion_result)
    output = capsys.readouterr().out
    assert output == assertion_message + "\n"

    ## Test in the notebook environment.
    nb_output = run_in_notebook("print_assertion",
                                "'{}'".format(status_text),
                                str(assertion_result))
    validate_notebook_output(nb_output,
                             assertion_message,
                             assertion_message_encoded)


def test_markdownmessage(markdown_text, markdown_text_encoded):
    md_obj = MarkdownMessage(markdown_text)
    assert md_obj.__repr__() == markdown_text
    assert md_obj._repr_markdown_() == markdown_text_encoded


def test_statusmessage(status_text,
                       status_message,
                       status_message_encoded,
                       monkeypatch,
                       mock_now):
    def mocknow():
        return mock_now
    monkeypatch.setattr(ip, "now", mocknow)

    md_obj = StatusMessage(status_text)
    assert md_obj.__repr__() == status_message
    assert md_obj._repr_markdown_() == status_message_encoded


def test_assertmessage(status_text,
                       assertion_result,
                       assertion_message,
                       assertion_message_encoded):
    md_obj = AssertMessage(status_text, assertion_result)
    assert md_obj.__repr__() == assertion_message
    assert md_obj._repr_markdown_() == assertion_message_encoded
