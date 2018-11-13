"""
Utilities for working with Pandas DataFrames.
"""

from pandas import DataFrame
from pandas import isna
from pandas.core.base import PandasObject
from pandas.api.types import (
    is_list_like,
    is_integer_dtype,
    is_float_dtype
)

import inspect
import warnings


## Simple warning message formatting.
def _simple_warn(msg, category=UserWarning, *args, **kwargs):
    """Show a simple warning of the form <type>: <warning message>."""
    print("{categ}: {msg}".format(categ=category.__name__, msg=msg))

warnings.showwarning = _simple_warn


def fmt_df(df,
           fmt_int=True,
           fmt_float=True,
           fmt_percent=None,
           fmt_dollar=None,
           precision=None):
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
    return FormattedDataFrame(df,
                              fmt_int=fmt_int,
                              fmt_float=fmt_float,
                              fmt_percent=fmt_percent,
                              fmt_dollar=fmt_dollar,
                              precision=precision)


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

    ## Register internal property names to work with DataFrame
    ## attribute methods.
    _metadata = [
        "_signature",
        "_dynamic_int",
        "_dynamic_float",
        "_column_precision",
        "_static_formatters",
        "_constructor_params"
    ]

    ## Manipulation result should remain a FormattedDataFrame.
    @property
    def _constructor(self):
        ## Use the same formatting args that were passed to the
        ## original instance.
        return lambda df: FormattedDataFrame(df, **self._constructor_params)

    def __init__(self,
                 df,
                 fmt_int=True,
                 fmt_float=True,
                 fmt_percent=None,
                 fmt_dollar=None,
                 precision=None):
        if not self._is_df_like(df):
            raise ValueError("'df' must be a DataFrame")
        super(FormattedDataFrame, self).__init__(data=df, copy=False)

        ## Cache the parameters passed to __init__ for use in the
        ## _constructor function.
        self._constructor_params = {
            "fmt_int": fmt_int,
            "fmt_float": fmt_float,
            "fmt_percent": fmt_percent,
            "fmt_dollar": fmt_dollar,
            "precision": precision
        }

        ## Cache the structure of the original DF to check for modification.
        self._signature = self._get_signature()

        if isinstance(precision, dict):
            for v in precision.values():
                if not isinstance(v, int):
                    raise ValueError("All column precision values must be int")
            self._column_precision = precision
        else:
            self._column_precision = {}

        ## Set up formatters for columns that requested static formatting.
        ## Note that the _*_formatter functions rely on the _column_precision
        ## attribute already being set above.
        formatters = {}

        def check_repeated_col(col, fmts):
            ## Check whether the given column already has formatting associated
            ## with it.
            if col in fmts:
                msg = "Column {} was supplied more than once to a" + \
                      "formatting parameter"
                raise ValueError(msg.format(col))

        self._dynamic_int = False
        if isinstance(fmt_int, bool) and fmt_int:
            self._dynamic_int = True
        else:
            int_cols = _col_name_list(fmt_int)
            for colname in int_cols:
                check_repeated_col(colname, formatters)
                formatters[colname] = self._int_formatter(colname)

        self._dynamic_float = False
        if isinstance(fmt_float, bool) and fmt_float:
            self._dynamic_float = True
        else:
            float_cols = _col_name_list(fmt_float)
            for colname in float_cols:
                check_repeated_col(colname, formatters)
                formatters[colname] = self._float_formatter(colname)

        pct_cols = _col_name_list(fmt_percent)
        for colname in pct_cols:
            check_repeated_col(colname, formatters)
            formatters[colname] = self._pct_formatter(colname)
        dollar_cols = _col_name_list(fmt_dollar)
        for colname in dollar_cols:
            check_repeated_col(colname, formatters)
            formatters[colname] = self._dollar_formatter(colname)

        ## Convert the format strings to formatting functions.
        self._static_formatters = self._ensure_fmt_funcs(formatters)

    @staticmethod
    def _is_df_like(df):
        """Check if an object is a 2D Pandas object (ie. a DataFrame).

        This cannot be accomplished just by checking `isinstance(df, DataFrame)`
        because of the use of utility classes like BlockManager.
        """
        if not isinstance(df, PandasObject):
            return False
        try:
            return df.ndim == 2
        except AttributeError:
            return False

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
                    ## Override the use of str.format().
                    return cls.NAN_STRING
                return fmt_spec.format(x)

            return fmt_num

        def ensure_fmt_func(fmtter):
            if isinstance(fmtter, str):
                return fmt_func(fmtter)
            return fmtter

        return {colname: ensure_fmt_func(fmtter)
                for colname, fmtter in fmt_dict.items()}

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
        ## If there are static formatters, warn if the DF has been modified
        ## in a way that might break the original formatting.
        if self._static_formatters and self._df_is_modified():
            warnings.warn(
                "The DF underlying this FormattedDataFrame instance" +
                " appears to have changed, and requested formatting may" +
                " no longer apply. It is recommended to create a new instance" +
                " by calling `fmt_df()`."
            )

        formatters = {}
        if self._dynamic_float:
            float_cols = self._detect_numeric_cols("float")
            for colname in float_cols:
                ## Formatting specified by column name overrides automatic
                ## detection.
                if colname not in self._static_formatters:
                    formatters[colname] = self._float_formatter(colname)
        if self._dynamic_int:
            int_cols = self._detect_numeric_cols("int")
            for colname in int_cols:
                ## Note that this will override float formatting in the case
                ## of int columns with NaNs.
                if colname not in self._static_formatters:
                    formatters[colname] = self._int_formatter(colname)
        formatters = self._ensure_fmt_funcs(formatters)
        formatters.update(self._static_formatters)
        return formatters

    def _detect_numeric_cols(self, col_type):
        """Find columns whose `dtype` matches the given type string.

        Returns a tuple of column names.
        """
        ## Use specific functions for int and float to include
        ## all related types.
        def _is_float_col(col):
            return is_float_dtype(self.dtypes[col])

        def _is_int_col(col):
            if is_integer_dtype(self.dtypes[col]):
                return True
            ## Columns that are integer except for the fact of having NaNs
            ## should be formatted as int (unless the column is all NaN).
            if _is_float_col(col):
                col_nona = self[col].dropna()
                if len(col_nona) > 0 and col_nona.apply(float.is_integer).all():
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
        ## Check whether formatters are already supplied.
        ## Do this by matching up supplied args against the signature
        ## of the overriden method.
        fmt_func = "to_{}".format(fmt)
        super_method = getattr(super(FormattedDataFrame, self), fmt_func)
        super_method_sig = inspect.signature(super_method)
        super_bound_args = super_method_sig.bind(*args, **kwargs).arguments
        supplied_fmt = super_bound_args.get("formatters", {})
        ## If supplied, the formatting arg could be either
        ## a list of length equal to the number of columns or a dict.
        ## If a list, this means a formatter is specified for each column.
        ## Since we are not overriding these, we delegate back to the
        ## super method.
        if isinstance(supplied_fmt, dict):
            ## Merge supplied formatters into the dict produced in this class.
            computed_fmt = self._get_formatters()
            ## Formatters supplied in args override default numeric formatting.
            computed_fmt.update(supplied_fmt)
            super_bound_args["formatters"] = computed_fmt

        return super_method(**super_bound_args)

    def to_string(self, *args, **kwargs):
        """Override `DataFrame.to_string()` to apply custom formatting."""
        return self._to_fmt_with_formatters("string", *args, **kwargs)

    def to_html(self, *args, **kwargs):
        """Override `DataFrame.to_html()` to apply custom formatting."""
        return self._to_fmt_with_formatters("html", *args, **kwargs)

    def _int_formatter(self, col_name):
        """Returns a format specifier string to be applied to int columns."""
        precision = self._column_precision.get(col_name,
                                               self.INT_DEFAULT_PRECISION)
        return "{{: ,.{prec}f}}".format(prec=precision)

    def _float_formatter(self, col_name):
        """Returns a format specifier string to be applied to float columns."""
        precision = self._column_precision.get(col_name,
                                               self.FLOAT_DEFAULT_PRECISION)
        return "{{: ,.{prec}f}}".format(prec=precision)

    def _pct_formatter(self, col_name):
        """Returns a format specifier string to be applied to percent columns."""
        precision = self._column_precision.get(col_name,
                                               self.PCT_DEFAULT_PRECISION)
        return "{{: ,.{prec}%}}".format(prec=precision)

    def _dollar_formatter(self, col_name):
        """Returns a format specifier string to be applied to dollar columns."""
        precision = self._column_precision.get(col_name,
                                               self.DOLLAR_DEFAULT_PRECISION)
        fmt_str = "{{: ,.{prec}f}}".format(prec=precision)

        def fmt_val(x):
            if isna(x):
                return self.NAN_STRING
            ## Insert a dollar sign after the positive/negative sign position.
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
        The argument supplied to a column name parameter, either a single column
        name or a list-like of column names.

    Returns
    -------
    list
        If `colname_arg` is a single string, returns a list containing it as a
        single element. Otherwise returns arg as a list. If anything else,
        returns an empty list.
    """
    if colname_arg:
        if isinstance(colname_arg, str):
            return [colname_arg]
        if is_list_like(colname_arg):
            return list(colname_arg)
    return []

