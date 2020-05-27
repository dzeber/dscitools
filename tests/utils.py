# Use `dill` rather than `pickle` to better handle functions and
# complex types.
import dill
from nbformat.v4 import new_notebook, new_code_cell
from nbconvert.preprocessors import ExecutePreprocessor


CODE_CELL_SOURCE = """
import dill
func, args = dill.loads({})
func(*args)
"""


def call_func_in_notebook(func, *args):
    """Call a function in a Jupyter notebook code cell and capture the output.

    This can be used for testing code that interacts with the notebook
    environment, eg. rich output formatting. It will call a single function,
    passing it the specified args.

    Parameters
    ----------
    func : function
        The function to run. It must be defined at the module level, and will be
        imported by name into the notebook.
    args : object
        Objects passed as args to the function.

    Returns
    -------
    list of dict
        The cell outputs in the Jupyter notebook format.
    """
    # Create a notebook with a single code cell that calls the function.
    nb = new_notebook()
    # Pass serialized versions of the objects into the notebook.
    pickle_str = dill.dumps((func, args))
    code_cell = new_code_cell(CODE_CELL_SOURCE.format(repr(pickle_str)))
    nb.cells.append(code_cell)
    # Run the notebook to capture the output.
    # Output will get written to the `code_cell` instance.
    ep = ExecutePreprocessor()
    ep.preprocess(nb)

    return code_cell.outputs


def get_notebook_rich_output(func, *args):
    """Call a function in a Jupyter notebook context and return the output data.

    The function is run using `call_func_in_notebook`. This validates that a
    single rich output was generated, and returns the output data as a dict
    mapping mime-type to value.
    Parameters

    ----------
    func : function
        The function to run. It must be defined at the module level, and will be
        imported by name into the notebook.
    args : object
        Objects passed as args to the function.

    Returns
    -------
    dict
        The rich output as a mapping of mime-type to value.
    """
    cell_outputs = call_func_in_notebook(func, *args)
    assert len(cell_outputs) == 1
    output = cell_outputs[0]
    assert output.get("output_type") == "display_data"
    return output.get("data", {})
