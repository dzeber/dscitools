"""
Utilities for working with Spark DataFrames.
"""


def dump_to_csv(df, path, write_mode=None, num_parts=1, compress=True):
    """Dump a DataFrame to CSV.

    The data is written to CSV in standard format with UTF-8 encoding by
    delegating to `df.write.csv()`. Null/missing values are encoded as the
    empty string.

    The CSV can optionally be compressed with gzip, or split into multiple part
    files.

    Parameters
    ----------
    df : DataFrame
        A Spark DataFrame to output as CSV.
    path : str
        A location on S3 or local file path.
    write_mode : str, optional
        The value passed to the `mode` parameters of `df.write.csv()`,
        defaulting to that function's default value (`"error"`). Overwriting an
        existing files must be forced by explicitly specifying `"overwrite"`.
    num_parts : int, optional
        The number of part files the CSV should be split into. They will be
        given default names.
    compress : bool, optional
        Should the output CSV files be compressed using gzip? Defaults to
        `True`.
    """
    df.repartition(num_parts)\
        .write.csv(path,
                   mode=write_mode,
                   compression="gzip" if compress else None,
                   header=True,
                   nullValue="",
                   emptyValue="",
                   encoding="utf-8")

