"""
Utilities for working with Spark DataFrames.
"""

import os
import os.path
import tempfile
import time


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


def _simplify_csv_local(output_dir, new_contents=[]):
    """Simplify local Spark output as described in `dump_to_csv()`.

    Restrict consideration to the specific new dir contents given.
    """
    new_csvs = []
    for fname in os.listdir(output_dir):
        if fname not in new_contents:
            ## Leave any pre-existing files as is.
            continue
        if fname.endswith(".csv") or fname.endswith(".csv.gz"):
            new_csvs.append(fname)
        else:
            ## This is a non-data file that was automatically generated
            ## by Spark.
            ## Delete.
            try:
                os.remove(os.path.join(output_dir, fname))
            except OSError:
                pass

    remaining_contents = os.listdir(output_dir)
    if remaining_contents == new_csvs and len(new_csvs) == 1:
        ## The only thing left in the dir is the new CSV.
        ## Replace the output dir with that file.
        output_file = new_csvs[0]
        output_path = os.path.join(output_dir, output_file)
        ## Move the CSV to a temp dir, and delete the original output dir.
        tmpdir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmpdir, output_file)
        os.rename(output_path, tmp_path)
        os.rmdir(output_dir)

        ## The output dir name will be used as the new filename.
        ## Attempt to make sure it has the appropriate extension.
        norm_path = os.path.normpath(output_dir)
        is_gzipped = output_file.endswith(".gz")
        if is_gzipped:
            if norm_path.endswith(".gz"):
                csv_file = norm_path
            elif norm_path.endswith(".csv"):
                csv_file = "{}.gz".format(norm_path)
            else:
                csv_file = "{}.csv.gz".format(norm_path)
        else:
            if norm_path.endswith(".csv"):
                csv_file = norm_path
            else:
                csv_file = "{}.csv".format(norm_path)
        ## However, don't overwrite if the new filename already exists.
        target_exists = os.path.exists(csv_file)
        if target_exists:
            print("Target file {} already exists.".format(csv_file))
            csv_file = norm_path
        ## Move the CSV back under the new filename.
        os.rename(tmp_path, csv_file)
        os.rmdir(tmpdir)
        print("{}CSV output was written to {}.".format(
            "Gzipped " if (target_exists and is_gzipped) else "",
            csv_file)
        )

    else:
        ## There are multiple files in the output dir.
        ## Rename the new CSV files to `partNNN.csv`.
        ## Make sure to avoid conflicting with any existing files.
        existing_part_nums = []
        for fname in remaining_contents:
            if fname.startswith("part"):
                fname = fname[4:]
            else:
                continue
            if fname.endswith(".csv"):
                fname = fname[:-4]
            elif fname.endswith(".csv.gz"):
                fname = fname[:-7]
            else:
                continue
            if not fname:
                continue
            try:
                part_num = int(fname)
                existing_part_nums.append(part_num)
            except ValueError:
                continue
        next_part_num = max(existing_part_nums) + 1 if existing_part_nums else 1
        new_csvs.sort()
        first_part_file = None
        for output_file in new_csvs:
            new_filename = "part{num}.csv{gz}".format(
                num=next_part_num,
                gz=".gz" if output_file.endswith(".gz") else ""
            )
            if first_part_file is None:
                first_part_file = new_filename
            os.rename(os.path.join(output_dir, output_file),
                      os.path.join(output_dir, new_filename))
            next_part_num += 1
        print("CSV output was written to {} across files {} to {}.".format(
            output_dir,
            first_part_file,
            new_filename))


def _simplify_csv_s3():
    """Simplify S3 Spark output as described in `dump_to_csv()`."""
    pass


def _is_s3_path(path):
    """Check if the given path should be considered an S3 path."""
    S3_PROTOCOL_PREFIXES = ["s3://", "s3a://", "s3n://"]
    for prefix in S3_PROTOCOL_PREFIXES:
        if path.startswith(prefix):
            return True
    return False


def dump_to_csv(df,
                path,
                write_mode=None,
                num_parts=1,
                compress=True,
                simplify=True):
    """Dump a DataFrame to CSV.

    The data is written to CSV in standard format with UTF-8 encoding by
    delegating to `df.write.csv()`. Null/missing values are encoded as the
    empty string.

    The CSV can optionally be compressed with gzip, or split into multiple part
    files.

    By default, Spark writes output to a dir with additional metadata and
    unweildy file names. This can optionally be simplified as follows:
    - After writing, if the dir contains only a single CSV file, simplification
      will remove the enclosing dir and replace it with the CSV file itself.
    - If there are multiple CSV files, simplification will remove any additional
      files generated by Spark and rename to the CSV part files with brief
      identifiers.
    Note that, if `num_parts` is 1 but the output is `append`-ed to a dir
    containing other CSV files, simplification considers this dir to contain
    multiple files.

    Parameters
    ----------
    df : DataFrame
        A Spark DataFrame to output as CSV.
    path : str
        A location on S3 or local file path representing a dir to write output
        to. If output results in a single CSV file and `simplify` is `True`,
        this will be interpreted as a single file, and extensions (`.csv`,
        `.gz`) will be appended if necessary.
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
    simplify : bool, optional
        Should the default Spark output be simplified? Defaults to `True`.
    """
    start_time = time.time()
    df.repartition(num_parts)\
        .write.csv(path,
                   mode=write_mode,
                   compression="gzip" if compress else None,
                   header=True,
                   nullValue="")
    ## Detect which files were newly written by Spark.
    new_contents = []
    for fname in os.listdir(path):
        fpath = os.path.join(path, fname)
        if os.stat(fpath).st_ctime > start_time:
            new_contents.append(fname)

    if simplify:
        if _is_s3_path(path):
            _simplify_csv_s3()
        else:
            _simplify_csv_local(path, new_contents=new_contents)


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
