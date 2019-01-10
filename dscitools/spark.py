"""
Utilities for working with Spark DataFrames.
"""


def show_df(df, n=10):
    """Show the first `n` rows of a Spark DF formatted as a Pandas DF.

    This provides a pretty-printed alternative to Spark's `DF.head()` and
    `DF.show()`.

    Parameters
    ----------
    df : DataFrame
        A Spark DataFrame.
    n : int, optional
        The number of rows to show (defaults to 10).

    Returns
    -------
    DataFrame
        A Pandas DF containing the first `n` rows of `df`.
    """
    return df.limit(n).toPandas()


def renew_cache(df):
    """Cache a Spark DF, unpersisting first if it is already cached.

    This helps avoid problems when rerunning code that caches a DF.

    Parameters
    ----------
    df : DataFrame
        A Spark DataFrame to be cached.

    Returns
    -------
    DataFrame
        The cached DataFrame.
    """
    if df.is_cached:
        df = df.unpersist()
    return df.cache()


def count_distinct(df, *cols):
    """Count distinct values across the given columns of a Spark DF.

    This triggers a job to run the count.

    Parameters
    ----------
    df : DataFrame
        A Spark DataFrame.
    cols : str, Column or list-like
        Zero or more string column names or Columns, or a list thereof.

    Returns
    -------
    int
        The number of distinct values in a single column, or the number of
        distinct rows across multiple columns. If no columns are given, returns
        the number of distinct rows in the DataFrame.
    """
    if len(cols) > 0:
        ## Delegate type handling for the col args to select().
        df = df.select(*cols)
    return df.distinct().count()


def get_colname(col):
    """Look up the name associated a Spark Column.

    This functions as an inverse of `pyspark.sql.functions.col()`.

    Parameters
    ----------
    col : Column
        A Spark DataFrame Column

    Returns
    -------
    str
        The column's name.
    """
    ## The name doesn't appear to be accessible as a property from the Python
    ## Column object.
    ## This mirrors what Column.__repr__ does.
    return col._jc.toString()

