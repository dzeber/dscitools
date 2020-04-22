import pickle

from nbformat.v4 import new_notebook, new_code_cell
from nbconvert.preprocessors import ExecutePreprocessor


CODE_CELL_SOURCE = """
import pickle
pickle_str = {}
func, args = pickle.loads(pickle_str)
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
    pickle_str = pickle.dumps((func, args))
    code_cell = new_code_cell(CODE_CELL_SOURCE.format(pickle_str))
    nb.cells.append(code_cell)
    # Run the notebook to capture the output.
    # Output will get written to the `code_cell` instance.
    ep = ExecutePreprocessor()
    ep.preprocess(nb)

    return code_cell.outputs
