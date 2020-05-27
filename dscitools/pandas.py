"""
Utilities for working with Pandas DataFrames.
"""

from __future__ import division, absolute_import
import inspect
import warnings

from pandas import DataFrame, get_option
try:
    from pandas.io.formats.format import DataFrameFormatter
except ImportError:
    from pandas.formats.format import DataFrameFormatter
try:
    from pandas import isna
except ImportError:
    # Fix for older versions of pandas
    from pandas import isnull

    isna = isnull
from pandas.api.types import is_list_like, is_integer_dtype, is_float_dtype
from numpy import isscalar

from .common import to_str, is_str


# Simple warning message formatting.
def _simple_warn(msg, category=UserWarning, *args, **kwargs):
    """Show a simple warning of the form <type>: <warning message>."""
    print("{categ}: {msg}".format(categ=category.__name__, msg=msg))


warnings.showwarning = _simple_warn


def fmt_df(
    df,
    fmt_int=True,
    fmt_float=True,
    fmt_percent=None,
    fmt_dollar=None,
    precision=None,
):
    """Wrap the given DataFrame so that it will print with number formatting.

    Parameters
    ----------
    df : DataFrame
        A DataFrame to be formatted.
    fmt_int : bool, str or list-like, optional
        Should integer formatting (comma-separation) be applied? If a
        single column name or list of column names, apply integer formatting
        to these columns. If `True`, detect integer columns on printing and
        apply formatting. If `False`, no integer formatting is applied.
    fmt_float : bool, str or list-like, optional
        Should float formatting (fixed number of decimal places) be
        applied? If a single column name or list of column names, apply
        float formatting to these columns. If `True`, detect float columns
        on printing and apply formatting. If `False`, no float formatting is
        applied.
    fmt_percent : str or list-like, optional
        A single column name or list or column names that formatting should
        be applied to. Percent formatting multiplies by 100, rounds to 2
        decimal places, and appends a percent sign.
    fmt_dollar : str or list-like, optional
        A single column name or list or column names that formatting should
        be applied to. Dollar formatting rounds to 2 decimal places and
        prepends a dollar sign.
    precision : dict, optional
        Dict mapping numeric column names to the number of decimal places they
        should be rounded to. Overrides default precision settings.

    Returns
    -------
    FormattedDataFrame
    """
    return FormattedDataFrame(
        df,
        fmt_int=fmt_int,
        fmt_float=fmt_float,
        fmt_percent=fmt_percent,
        fmt_dollar=fmt_dollar,
        precision=precision,
    )


def fmt_count_df(
    df,
    n_overall=None,
    count_col=None,
    order_by_count=False,
    show_cum_pct=False,
    pct_col_name=None,
    fmt=True,
    pct_precision=2,
):
    """Format a DataFrame for displaying group counts.

    This is useful for presenting results or diagnostics in the form of a table
    listing counts corresponding to groups or cases. Percentages corresponding
    to the counts are appended, and numbers are formatted. The DF can
    optionally be ordered by decreasing count and include cumulative
    percentages down the rows. The original DF is not modified.

    Any columns added to the DF are assigned default names unless overridden
    using the `pct_col_name` parameter.

    Parameters
    ----------
    df : DataFrame
        A DataFrame containing counts to be formatted.
    n_overall : numeric, str, or list-like, optional
        The overall total (denominator) to use in computing percentages.
        Supply:
            - a number representing the total
            - the name of a column containing individual row-wise totals. In
              this case, the count column is divided element-wise by this
              column.
            - `None` (the default), indicating that the sum of the
              corresponding count column is to be used
        If multiple count columns are specified, this should be a list-like
        containing an element corresponding to each count column, in the same
        order, with elements as described above. In this case, a single `None`
        is interpreted as using `None` for each count column.
    count_col : str or list-like, optional
        The count column(s) for which percentages should be computed:
            - a single column name
            - a list-like of column names
            - `None` (the default), in which case any columns named "count",
              "n", "num", or starting with "n_" or "num_" are used
    order_by_count : bool, str or list-like, optional
        Should the final DF be reordered by decreasing count? Supply:
            - a boolean indicating whether the result should be reorded. If
              `True`, the first count column is used for ordering.
            - the name of a count column to order by.
            - a list-like of count columns to order by, each in decreasing
              order.
    show_cum_pct : bool, str or list-like, optional
        Should cumulative percentages (accumulating down the rows) be included
        in the final table? This is done after reordering by decreasing count,
        if required. Supply:
            - the name of a count column, or a list-like of count column names,
              to include cumulative percentages for
            - a boolean indicating whether to include cumulative percentages.
              If `True`, they are included for each count column.
        Names for these columns are generated automatically. If the
        corresponding `n_overall` for one of these is a column of totals rather
        than a scalar, the cumulative percent column will be omitted.
    pct_col_name : str or list-like, optional
        The name(s) to use for the percentage column(s). Supply:
            - the column name to use, if there is a single count columm.
            - a list-like of column names, if there are multiple count columns.
              In this case, the list should contain a name corresponding to
              each count column, in the same order.
        A single `None` or a `None` list element indicates that default naming
        is to be used for that column.
    fmt : bool
        Should number formatting be applied? If `True`, the result is wrapped
        in a `FormattedDataFrame`. Note that the percentage columns actually
        contain proportions between 0 and 1, and are displayed as percentages
        if `fmt` is `True`.
    pct_precision : int, optional
        The number of decimal places the formatted percentages should be
        rounded to.

    Returns
    -------
    FormattedDataFrame or DataFrame
        A copy of `df` structured as described above, possibly with additional
        columns. If `fmt` is `True`, it is wrapped in a `FormattedDataFrame`.
    """
    if count_col is None:
        # Detect count columns.
        def is_count_col(colname):
            return (
                colname in ("count", "n", "num")
                or colname.startswith("n_")
                or colname.startswith("num_")
            )

        count_col = [col for col in df.columns if is_count_col(col)]
    else:
        count_col = _col_name_list(count_col)
        for col in count_col:
            if col not in df.columns:
                err_msg = "Count column '{}' was not found in the DataFrame"
                raise ValueError(err_msg.format(col))
    if len(count_col) > len(set(count_col)):
        # This will cause a problem matching against other params by index.
        # Rather than just dropping duplicate column names, throw an error.
        raise ValueError("Cannot handle repeated count column names")

    if n_overall is None:
        n_overall = [None] * len(count_col)
    elif not is_list_like(n_overall):
        n_overall = [n_overall]
    if len(n_overall) != len(count_col):
        err_msg = (
            "If specified, there must be as many `n_overall` values"
            + " as count columns"
        )
        raise ValueError(err_msg)

    if pct_col_name is None:
        pct_col_name = [None] * len(count_col)
    elif not is_list_like(pct_col_name):
        pct_col_name = [pct_col_name]
    if len(pct_col_name) != len(count_col):
        err_msg = (
            "If specified, there must be as many `pct_col_name`"
            + " values as count columns"
        )
        raise ValueError(err_msg)

    order_cols = []
    if order_by_count == True:  # noqa: E712
        order_cols.append(count_col[0])
    else:
        # Fail silently if these are not valid count column names.
        order_by_count = _col_name_list(order_by_count)
        for col in order_by_count:
            if col in count_col:
                order_cols.append(col)

    cum_pct_col_names = []
    if show_cum_pct == True:  # noqa: E712
        cum_pct_col_names = count_col
    else:
        # Fail silently if these are not valid count column names.
        show_cum_pct = _col_name_list(show_cum_pct)
        for col in show_cum_pct:
            if col in count_col:
                cum_pct_col_names.append(col)

    pct_df = df.copy()
    if order_cols:
        pct_df.sort_values(order_cols, ascending=False, inplace=True)

    # This will be a list of (<pct column name>, <computed pct column>).
    pct_cols = []
    # This will a similar list for cumulative pct columns.
    cum_pct_cols = []
    for i, col in enumerate(count_col):
        denom = n_overall[i]
        if denom is None:
            denom = df[col].sum()
        elif is_str(denom):
            # Interpret as a column name.
            if denom not in df.columns:
                err_msg = (
                    "Totals ('n_overall') column '{}' was not found"
                    + "in the DataFrame"
                )
                raise ValueError(err_msg.format(denom))
            denom = df[denom]
        pct = df[col] / denom

        if pct_col_name[i] is None:
            # Generate a default name for the percentage column.
            def prop_col_name(col):
                if col == "count":
                    return "proportion"
                if col == "n":
                    return "p"
                if col == "num":
                    return "prop"
                if col.startswith("n_"):
                    return "p_" + col[2:]
                if col.startswith("num_"):
                    return "prop_" + col[4:]
                return "prop_" + col

            pct_colname = prop_col_name(col)
        else:
            pct_colname = to_str(pct_col_name[i])

        pct_cols.append((pct_colname, pct))

        # Omit cumulative percent columns for which n_overall is not scalar.
        if col in cum_pct_col_names and isscalar(denom):
            cumpct = df[col].cumsum() / denom
            cumpct_colname = "cum " + pct_colname
            cum_pct_cols.append((cumpct_colname, cumpct))

    all_pct_cols = []
    for colname, col in pct_cols:
        if colname in pct_df.columns:
            err_msg = (
                "Attempting to add percent column '{}', but a column"
                + " with this name is already in the DataFrame"
            )
            raise ValueError(err_msg.format(colname))
        pct_df[colname] = col
        all_pct_cols.append(colname)
    for colname, col in cum_pct_cols:
        if colname in pct_df.columns:
            err_msg = (
                "Attempting to add cumulative percent column '{}',"
                + " but a column with this name is already in the"
                + " DataFrame"
            )
            raise ValueError(err_msg.format(colname))
        pct_df[colname] = col
        all_pct_cols.append(colname)

    if fmt:
        precision_arg = {col: pct_precision for col in all_pct_cols}
        pct_df = FormattedDataFrame(
            pct_df, fmt_percent=all_pct_cols, precision=precision_arg
        )
    return pct_df


class FormattedDataFrame(DataFrame):
    """A wrapper for a Pandas DataFrame that renders with custom formatting.

    When instances of this class are printed, numeric columns are formatted for
    pretty-printing.

    The formatting behaviour can be dynamic, detecting numeric columns each
    time it is printed, or static, specifying formatting by column name. In the
    static case, a new instance should be created each time the underlying
    DataFrame is modified.

    Parameters
    ----------
    df : DataFrame
        A DataFrame to be formatted. The new instance wraps `df` without
        copying.
    fmt_int : bool, str or list-like, optional
        Should integer formatting (comma-separation) be applied? If a
        single column name or list of column names, apply integer formatting
        to these columns. If `True`, detect integer columns on printing and
        apply formatting. If `False`, no integer formatting is applied.
    fmt_float : bool, str or list-like, optional
        Should float formatting (fixed number of decimal places) be
        applied? If a single column name or list of column names, apply
        float formatting to these columns. If `True`, detect float columns
        on printing and apply formatting. If `False`, no float formatting is
        applied.
    fmt_percent : str or list-like, optional
        A single column name or list or column names that formatting should
        be applied to. Percent formatting multiplies by 100, rounds to 2
        decimal places, and appends a percent sign.
    fmt_dollar : str or list-like, optional
        A single column name or list or column names that formatting should
        be applied to. Dollar formatting rounds to 2 decimal places and
        prepends a dollar sign.
    precision : dict, optional
        Dict mapping numeric column names to the number of decimal places they
        should be rounded to. Overrides default precision settings.
    """

    INT_DEFAULT_PRECISION = 0
    FLOAT_DEFAULT_PRECISION = 2
    PCT_DEFAULT_PRECISION = 2
    DOLLAR_DEFAULT_PRECISION = 2
    NAN_STRING = "NaN"

    # Register internal property names to work with DataFrame
    # attribute methods.
    _metadata = [
        "_signature",
        "_dynamic_int",
        "_dynamic_float",
        "_column_precision",
        "_static_formatters",
        "_constructor_params",
    ]

    # Manipulation result should remain a FormattedDataFrame.
    @property
    def _constructor(self):
        # Use the same formatting args that were passed to the
        # original instance.
        return lambda df: FormattedDataFrame(df, **self._constructor_params)

    def __init__(
        self,
        df,
        fmt_int=True,
        fmt_float=True,
        fmt_percent=None,
        fmt_dollar=None,
        precision=None,
    ):
        super(FormattedDataFrame, self).__init__(data=df, copy=False)

        # Cache the parameters passed to __init__ for use in the
        # _constructor function.
        self._constructor_params = {
            "fmt_int": fmt_int,
            "fmt_float": fmt_float,
            "fmt_percent": fmt_percent,
            "fmt_dollar": fmt_dollar,
            "precision": precision,
        }

        # Cache the structure of the original DF to check for modification.
        self._signature = self._get_signature()

        if precision or precision == 0:
            # Explicitly include numeric 0 in type check
            try:
                precision = dict(precision)
            except TypeError:
                raise ValueError("If given, 'precision' must be a dict")
            try:
                for col in precision:
                    precision[col] = int(precision[col])
            except (TypeError, ValueError):
                raise ValueError("Precision values must be integers")
            self._column_precision = precision
        else:
            self._column_precision = {}

        # Set up formatters for columns that requested static formatting.
        # Note that the _*_formatter functions rely on the _column_precision
        # attribute already being set above.
        formatters = {}

        def ensure_col_not_repeated(col, fmts):
            # Check whether the given column already has formatting associated
            # with it.
            if col in fmts:
                msg = (
                    "Column {} was supplied more than once to a"
                    + "formatting parameter"
                )
                raise ValueError(msg.format(col))

        self._dynamic_int = False
        if fmt_int == True:  # noqa: E712

            # Check for an explicit boolean value.
            self._dynamic_int = True
        else:
            int_cols = _col_name_list(fmt_int)
            for colname in int_cols:
                ensure_col_not_repeated(colname, formatters)
                formatters[colname] = self._int_formatter(colname)

        self._dynamic_float = False
        if fmt_float == True:  # noqa: E712
            # Check for an explicit boolean value.
            self._dynamic_float = True
        else:
            float_cols = _col_name_list(fmt_float)
            for colname in float_cols:
                ensure_col_not_repeated(colname, formatters)
                formatters[colname] = self._float_formatter(colname)

        pct_cols = _col_name_list(fmt_percent)
        for colname in pct_cols:
            ensure_col_not_repeated(colname, formatters)
            formatters[colname] = self._pct_formatter(colname)
        dollar_cols = _col_name_list(fmt_dollar)
        for colname in dollar_cols:
            ensure_col_not_repeated(colname, formatters)
            formatters[colname] = self._dollar_formatter(colname)

        # Convert the format strings to formatting functions.
        self._static_formatters = self._ensure_fmt_funcs(formatters)

    def _get_signature(self):
        """Generate a signature used to check for modifications to the DF."""
        return self.dtypes.to_dict()

    @classmethod
    def _ensure_fmt_funcs(cls, fmt_dict):
        """Convert any formatting strings to functions in a formatting dict.

        Parameters
        ----------
        fmt_str_dict : dict
            A dict mapping column names to formatting functions or format
            specifier strings.

        Returns
        -------
        dict
            The same dict with any formatting strings transformed to functions.
            Any existing functions are left as-is.
        """

        def fmt_func(fmt_spec):
            def fmt_num(x):
                if isna(x):
                    # Override the use of str.format().
                    return cls.NAN_STRING
                return fmt_spec.format(x)

            return fmt_num

        def ensure_fmt_func(fmtter):
            if is_str(fmtter):
                return fmt_func(fmtter)
            return fmtter

        return {
            colname: ensure_fmt_func(fmtter)
            for colname, fmtter in fmt_dict.items()
        }

    def _df_is_modified(self):
        """Check whether the underlying DF appears to have been modified.

        Returns `True` if the the DF currently has a column name that was not
        present when this FormattedDataFrame was created, or if one of the
        columns now has a different type; `False` otherwise.

        In particular, returns `False` if the DF is a subset of the original
        columns.
        """
        current = self._get_signature()
        for colname in current:
            if colname not in self._signature:
                return True
            if current[colname] != self._signature[colname]:
                return True
        return False

    def _get_formatters(self):
        """Generate a dict of formatters than can be passed to `to_string()`.

        Detects int or float columns, if these are to be formatted dynamically,
        and combines with static formatting rules. Static rules take precedence
        over dynamic ones.

        Returns
        -------
        dict
            A dict mapping column names to formatting functions that will be
            applied to individual column entries.
        """
        # If there are static formatters, warn if the DF has been modified
        # in a way that might break the original formatting.
        if self._static_formatters and self._df_is_modified():
            modified_msg = (
                "The DF underlying this FormattedDataFrame"
                + " instance appears to have changed, and requested "
                + " formatting may no longer apply. It is recommended to"
                + " create a new instance by calling `fmt_df()`."
            )
            warnings.warn(modified_msg)

        formatters = {}
        if self._dynamic_float:
            float_cols = self._detect_numeric_cols("float")
            for colname in float_cols:
                # Formatting specified by column name overrides automatic
                # detection.
                if colname not in self._static_formatters:
                    formatters[colname] = self._float_formatter(colname)
        if self._dynamic_int:
            int_cols = self._detect_numeric_cols("int")
            for colname in int_cols:
                # Note that this will override float formatting in the case
                # of int columns with NaNs.
                if colname not in self._static_formatters:
                    formatters[colname] = self._int_formatter(colname)
        formatters = self._ensure_fmt_funcs(formatters)
        formatters.update(self._static_formatters)
        return formatters

    def _detect_numeric_cols(self, col_type):
        """Find columns whose `dtype` matches the given type string.

        Returns a tuple of column names.
        """
        # Use specific functions for int and float to include
        # all related types.
        def _is_float_col(col):
            return is_float_dtype(self.dtypes[col])

        def _is_int_col(col):
            if is_integer_dtype(self.dtypes[col]):
                return True
            # Columns that are integer except for the fact of having NaNs
            # should be formatted as int (unless the column is all NaN).
            if _is_float_col(col):
                col_nona = self[col].dropna()
                if (
                    len(col_nona) > 0
                    and col_nona.apply(float.is_integer).all()
                ):
                    return True
            return False

        if col_type == "int":
            cols_of_type = [col for col in self.columns if _is_int_col(col)]
        elif col_type == "float":
            cols_of_type = [col for col in self.columns if _is_float_col(col)]
        else:
            cols_of_type = self.select_dtypes(include=col_type).columns
        return tuple(cols_of_type)

    def _to_fmt_with_formatters(self, fmt, *args, **kwargs):
        """Format the DF using the instance's custom number formatting.

        Calls the underlying DF's `to_*()` method, passing the formatting rules
        computed by `_get_formatters()` to the `formatters` parameter. Any args
        supplied explicitly to the function call are preserved and override the
        instance formatting.

        Parameters
        ----------
        fmt : str
            An output format type, ie. the `*` in a `to_*()` function.
        args, kwargs
            Other args passed to `to_*()`.
        """
        # Check whether formatters are already supplied.
        # Do this by matching up supplied args against the signature
        # of the overriden method.
        fmt_func = "to_{}".format(fmt)
        super_method = getattr(super(FormattedDataFrame, self), fmt_func)
        try:
            super_method_sig = inspect.signature(super_method)
            super_bound_args = super_method_sig.bind(*args, **kwargs).arguments
        except AttributeError:
            # Compat with Python < 3.5
            super_bound_args = inspect.getcallargs(
                super_method, *args, **kwargs
            )
            del super_bound_args["self"]
        supplied_fmt = super_bound_args.get("formatters")
        if not supplied_fmt:
            # Compat:
            # With signature(), if the 'formatters' arg is not supplied,
            # it will not be in super_bound_args.
            # With getcallargs(), it will have an entry in super_bound_args
            # with value None.
            supplied_fmt = {}
        # If supplied, the formatting arg could be either
        # a list of length equal to the number of columns or a dict.
        # If a list, this means a formatter is specified for each column.
        # Since we are not overriding these, we delegate back to the
        # super method.
        if isinstance(supplied_fmt, dict):
            # Merge supplied formatters into the dict produced in this class.
            computed_fmt = self._get_formatters()
            # Formatters supplied in args override default numeric formatting.
            computed_fmt.update(supplied_fmt)
            super_bound_args["formatters"] = computed_fmt

        return super_method(**super_bound_args)

    def to_string(self, *args, **kwargs):
        """Override `DataFrame.to_string()` to apply custom formatting."""
        return self._to_fmt_with_formatters("string", *args, **kwargs)

    def to_html(self, *args, **kwargs):
        """Override `DataFrame.to_html()` to apply custom formatting."""
        return self._to_fmt_with_formatters("html", *args, **kwargs)

    def _repr_html_(self):
        """Show custom formatting in the notebook.

        This is the function called to produce HTML display in a Jupyter
        notebook. In pandas<0.25.1, this delegated to `to_html`, automatically
        pulling in FormattedDataFrame modifications. As of pandas 0.25.1,
        `DataFrame._repr_html_` creates a `DataFrameFormatter` directly
        without calling `to_html`, because `to_html` doesn't allow for setting
        a minimum number of rows.

        Override `_repr_html_` to preserve the underlying functionality for
        `DataFrame`s while pulling in the custom formatters when rendering
        in the notebook.
        """
        if not self._info_repr() and get_option("display.notebook_repr_html"):
            kwargs = {
                "columns": None,
                "col_space": None,
                "na_rep": "NaN",
                "formatters": self._get_formatters(),
                "float_format": None,
                "sparsify": None,
                "justify": None,
                "index_names": True,
                "header": True,
                "index": True,
                "bold_rows": True,
                "escape": True,
                "max_rows": get_option("display.max_rows"),
                "max_cols": get_option("display.max_columns"),
                "show_dimensions": get_option("display.show_dimensions"),
                "decimal": ".",
                "table_id": None,
                "render_links": False,
            }
            try:
                min_rows = get_option("display.min_rows")
                kwargs["min_rows"] = min_rows
            except KeyError:
                pass

            formatter = DataFrameFormatter(self, **kwargs)
            html_result = formatter.to_html(notebook=True)
            if not html_result:
                # Compat with pandas<1.0
                html_result = formatter.buf.getvalue()
            return html_result
        else:
            super(FormattedDataFrame, self)._repr_html_()

    def _int_formatter(self, col_name):
        """Returns the format specifier string applied to int columns."""
        precision = self._column_precision.get(
            col_name, self.INT_DEFAULT_PRECISION
        )
        return "{{: ,.{prec}f}}".format(prec=precision)

    def _float_formatter(self, col_name):
        """Returns the format specifier string applied to float columns."""
        precision = self._column_precision.get(
            col_name, self.FLOAT_DEFAULT_PRECISION
        )
        return "{{: ,.{prec}f}}".format(prec=precision)

    def _pct_formatter(self, col_name):
        """Returns the format specifier string applied to percent columns."""
        precision = self._column_precision.get(
            col_name, self.PCT_DEFAULT_PRECISION
        )
        return "{{: ,.{prec}%}}".format(prec=precision)

    def _dollar_formatter(self, col_name):
        """Returns the format specifier string applied to dollar columns."""
        precision = self._column_precision.get(
            col_name, self.DOLLAR_DEFAULT_PRECISION
        )
        fmt_str = "{{: ,.{prec}f}}".format(prec=precision)

        def fmt_val(x):
            if isna(x):
                return self.NAN_STRING
            # Insert a dollar sign after the positive/negative sign position.
            x_fmt = fmt_str.format(x)
            if len(x_fmt) > 0:
                x_fmt = x_fmt[0] + "$" + x_fmt[1:]
            return x_fmt

        return fmt_val


def _col_name_list(colname_arg):
    """Ensure supplied column names are represented as a list or tuple.

    Parameters
    ----------
    colname_arg : str or list-like
        The argument supplied to a column name parameter, either a single
        column name or a list-like of column names.

    Returns
    -------
    list of str
        If `colname_arg` is a scalar value, returns a list containing it as a
        single string. Otherwise returns arg, ensuring elements are strings.
        If arg is falsey, returns an empty list.
    """
    if not colname_arg:
        return []
    if is_list_like(colname_arg):
        return [to_str(col) for col in colname_arg]
    return [to_str(colname_arg)]
