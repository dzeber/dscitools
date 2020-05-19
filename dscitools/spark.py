"""
Utilities for working with Spark DataFrames.
"""

import os
import os.path
import tempfile
import time
import boto3
import datetime
from botocore.exceptions import ClientError


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
        # Delegate type handling for the col args to select().
        df = df.select(*cols)
    return df.distinct().count()


def _simplify_csv_local(output_dir, new_files_since=0):
    """Simplify local Spark output as described in `dump_to_csv()`.

    Optionally restrict consideration to recently written files. Deletes
    non-data files (ie. Spark artefacts) and simplifies the path, for a single
    file, or the filenames, for multiple parts.

    Parameters
    ----------
    output_dir : str
        The dir path where the output was written.
    new_files_since : float
        A timestamp in seconds representing the earliest write date to consider.
        Simplifaction is restricted to files written since this datetime.
    """
    # Detect keys written since the given time cutoff.
    new_files = []
    for fname in os.listdir(output_dir):
        fpath = os.path.join(output_dir, fname)
        if os.path.isfile(fpath) and os.stat(fpath).st_ctime > new_files_since:
            new_files.append(fname)
    # Identify the data files, and delete the rest.
    new_csvs = []
    for fname in new_files:
        if fname.endswith(".csv") or fname.endswith(".csv.gz"):
            new_csvs.append(fname)
        else:
            # This is a non-data file that was automatically generated
            # by Spark.
            # Delete.
            try:
                os.remove(os.path.join(output_dir, fname))
            except OSError:
                pass

    remaining_contents = os.listdir(output_dir)
    if len(new_csvs) == 1 and remaining_contents == new_csvs:
        # The only thing left in the dir is the new CSV.
        # Replace the output dir with that file.
        output_file = new_csvs[0]
        output_path = os.path.join(output_dir, output_file)
        # Move the CSV to a temp dir, and delete the original output dir.
        tmpdir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmpdir, output_file)
        os.rename(output_path, tmp_path)
        os.rmdir(output_dir)

        # The output dir name will be used as the new filename.
        # Attempt to make sure it has the appropriate extension.
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
        # However, don't overwrite if the new filename already exists.
        target_exists = os.path.exists(csv_file)
        if target_exists:
            print("Target file {} already exists.".format(csv_file))
            csv_file = norm_path
        # Move the CSV back under the new filename.
        os.rename(tmp_path, csv_file)
        os.rmdir(tmpdir)
        print(
            "{}CSV output was written to {}.".format(
                # Mention compression explicitly if the output filename doesn't
                # include the '.gz' extension.
                "Gzipped " if (target_exists and is_gzipped) else "",
                csv_file,
            )
        )

    else:
        # There are multiple files in the output dir.
        # Rename the new CSV files to `partNNN.csv`.
        # Make sure to avoid conflicting with any existing files.
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
        next_part_num = (
            max(existing_part_nums) + 1 if existing_part_nums else 1
        )
        new_csvs.sort()
        first_part_file = None
        for output_file in new_csvs:
            new_filename = "part{num}.csv{gz}".format(
                num=next_part_num,
                gz=".gz" if output_file.endswith(".gz") else "",
            )
            if first_part_file is None:
                first_part_file = new_filename
            os.rename(
                os.path.join(output_dir, output_file),
                os.path.join(output_dir, new_filename),
            )
            next_part_num += 1
        print(
            "CSV output was written to {} across files {} to {}.".format(
                output_dir, first_part_file, new_filename
            )
        )


def _s3_prefix_exists(s3, bucket, prefix):
    """Check if the given prefix exists in a S3 bucket."""
    # Check if the prefix either matches an object exactly or is part of a
    # longer key.
    if prefix.endswith("/"):
        prefix = prefix[:-1]

    try:
        # Request metadata, treating `prefix` as an object key.
        # This will fail if the object doesn't exist.
        s3.head_object(Bucket=bucket, Key=prefix)
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            return True
    else:
        return True

    # Otherwise, check if it is part of a longer key.
    sub_objects = s3.list_objects_v2(
        Bucket=bucket, Prefix=prefix + "/", Delimiter="/", MaxKeys=1
    )
    return sub_objects["KeyCount"] > 0


def _simplify_csv_s3(s3_path, new_keys_since=0):
    """Simplify Spark output on S3 as described in `dump_to_csv()`.

    Optionally restrict consideration to recently written keys. Deletes non-data
    files (ie. Spark artefacts) and simplifies the path, for a single file, or
    the filenames, for multiple parts.

    Parameters
    ----------
    s3_path : str
        The S3 path where the output was written ("s3://<bucket>/...").
    new_keys_since : float
        A timestamp in seconds representing the earliest write date to consider.
        Simplifaction is restricted to keys written since this datetime.
    """
    s3 = boto3.client("s3")
    base_path = s3_path[(s3_path.find("://") + 3):]
    bucket, output_dir_prefix = base_path.split("/", 1)
    if not output_dir_prefix.endswith("/"):
        output_dir_prefix += "/"

    # Detect keys written since the given time cutoff.
    new_keys = []
    start_time_utc = datetime.datetime.utcfromtimestamp(new_keys_since)
    # Using `Delimiter="/"` means non-recursive listing, ie. up to and
    # including the next "/" in their key, if any.
    contents = s3.list_objects_v2(
        Bucket=bucket, Prefix=output_dir_prefix, Delimiter="/", MaxKeys=1000
    )
    # Assume we are working with relatively few keys.
    # Otherwise, fail out of simplification for now.
    if contents["IsTruncated"]:
        raise NotImplementedError(
            "Prefix {} matches >1000 objects.".format(output_dir_prefix)
        )
    # Contents contains only actual keys, not common prefixes ("subdirs").
    for obj in contents.get("Contents", []):
        # To compare datetimes, need to remove timezone from the
        # S3 last modified times.
        if obj["LastModified"].replace(tzinfo=None) > start_time_utc:
            new_keys.append(obj["Key"])
    # Identify the data files, and delete the rest.
    new_csvs = []
    to_delete = []
    for key in new_keys:
        if key.endswith(".csv") or key.endswith(".csv.gz"):
            new_csvs.append(key)
        else:
            # This is a non-data file that was automatically generated
            # by Spark.
            # Delete.
            to_delete.append(key)
    s3.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": k} for k in to_delete]}
    )

    # Check if the output "dir" has any further subkeys beyond the CSVs,
    # including sub-"directories".
    remaining_contents = s3.list_objects_v2(
        Bucket=bucket, Prefix=output_dir_prefix, Delimiter="/"
    )
    remaining_keys = [
        obj["Key"] for obj in remaining_contents.get("Contents", [])
    ]
    if (
        len(new_csvs) == 1
        and not remaining_contents["IsTruncated"]
        and not remaining_contents.get("CommonPrefixes")
        and remaining_keys == new_csvs
    ):
        # The only thing left in the dir is the new CSV.
        # Replace the output dir key with that file.
        output_file = new_csvs[0]
        # The output dir name will be used as the new filename.
        # Attempt to make sure it has the appropriate extension.
        target_key = output_dir_prefix.rstrip("/")
        is_gzipped = output_file.endswith(".gz")
        if is_gzipped:
            if target_key.endswith(".gz"):
                csv_file = target_key
            elif target_key.endswith(".csv"):
                csv_file = "{}.gz".format(target_key)
            else:
                csv_file = "{}.csv.gz".format(target_key)
        else:
            if target_key.endswith(".csv"):
                csv_file = target_key
            else:
                csv_file = "{}.csv".format(target_key)
        # However, don't overwrite if the new filename already exists.
        target_exists = (csv_file != target_key) and _s3_prefix_exists(
            s3, bucket, csv_file
        )
        if target_exists:
            print("Target key {} already exists.".format(csv_file))
            csv_file = target_key
        # Move the CSV to the new key.
        s3.copy({"Bucket": bucket, "Key": output_file}, bucket, csv_file)
        s3.delete_object(Bucket=bucket, Key=output_file)
        print(
            "{}CSV output was written to {}.".format(
                # Mention compression explicitly if the output key doesn't
                # include the '.gz' extension.
                "Gzipped " if (target_exists and is_gzipped) else "",
                csv_file,
            )
        )

    else:
        # Avoid dealing with pagination for now.
        # Just fail out of simplification if there are too many keys.
        if remaining_contents["IsTruncated"]:
            raise NotImplementedError(
                "Prefix {} matches too many objects.".format(output_dir_prefix)
            )
        # There are multiple files in the output dir.
        # Rename the new CSV files to `partNNN.csv`.
        # Make sure to avoid conflicting with any existing files.
        remaining_prefixes = [
            p["Prefix"] for p in remaining_contents.get("CommonPrefixes", [])
        ]
        all_objects = remaining_prefixes + remaining_keys
        existing_part_nums = []
        for key in all_objects:
            # Chop off the intial dir prefix and the final "/", if any.
            key = key[len(output_dir_prefix):].rstrip("/")
            if key.startswith("part"):
                key = key[4:]
            else:
                continue
            if key.endswith(".csv"):
                key = key[:-4]
            elif key.endswith(".csv.gz"):
                key = key[:-7]
            else:
                continue
            if not key:
                continue
            try:
                part_num = int(key)
                existing_part_nums.append(part_num)
            except ValueError:
                continue
        next_part_num = (
            max(existing_part_nums) + 1 if existing_part_nums else 1
        )
        new_csvs.sort()
        first_part_file = None
        for output_file in new_csvs:
            new_filename = "part{num}.csv{gz}".format(
                num=next_part_num,
                gz=".gz" if output_file.endswith(".gz") else "",
            )
            if first_part_file is None:
                first_part_file = new_filename
            s3.copy(
                {"Bucket": bucket, "Key": output_file},
                bucket,
                output_dir_prefix + new_filename,
            )
            s3.delete_object(Bucket=bucket, Key=output_file)
            next_part_num += 1
        print(
            "CSV output was written to {} across files {} to {}.".format(
                output_dir_prefix, first_part_file, new_filename
            )
        )


def _is_s3_path(path):
    """Check if the given path should be considered an S3 path."""
    S3_PROTOCOL_PREFIXES = ["s3://", "s3a://", "s3n://"]
    for prefix in S3_PROTOCOL_PREFIXES:
        if path.startswith(prefix):
            return True
    return False


def dump_to_csv(
    df, path, write_mode=None, num_parts=1, compress=True, simplify=True
):
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
    df.repartition(num_parts).write.csv(
        path,
        mode=write_mode,
        compression="gzip" if compress else None,
        header=True,
        nullValue="",
    )
    if not simplify:
        # We are done.
        return

    # Otherwise, run the appropriate simplfication logic.
    if _is_s3_path(path):
        _simplify_csv_s3(path, start_time)
    else:
        # Detect which files/objects were newly written by Spark.
        _simplify_csv_local(path, start_time)


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
    # The name doesn't appear to be accessible as a property from the Python
    # Column object.
    # This mirrors what Column.__repr__ does.
    return col._jc.toString()
