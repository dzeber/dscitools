import pytest
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal
import gzip
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import boto3
from moto import mock_s3

from dscitools.spark import (
    show_df,
    renew_cache,
    get_colname,
    count_distinct,
    dump_to_csv,
    _simplify_csv_s3
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession
        .builder
        .master("local")
        .appName("dscitools_test")
        ## Requirement to get Spark to work with S3.
        #.config("spark.jars.packages",
        #        "org.apache.hadoop:hadoop-aws:2.7.3")
        .getOrCreate()
    )

    yield spark

    ## Teardown
    spark.stop()


@pytest.fixture
def pandas_df():
    return DataFrame({
        "label": ["a", "a", "a", "b", "b", "b", "c", "c"],
        "value": [1, 2, 3, 4, 5, 1, 2, 3]
    })


@pytest.fixture
def spark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


@pytest.fixture
def csv_rows():
    return [
        "label,value",
        "a,1",
        "a,2",
        "a,3",
        "b,4",
        "b,5",
        "b,1",
        "c,2",
        "c,3"
    ]


def test_show_df(spark_df, pandas_df):
    assert_frame_equal(show_df(spark_df), pandas_df[:10])
    assert_frame_equal(show_df(spark_df, 5), pandas_df[:5])


def test_renew_cache(spark_df):
    ## Make sure we are starting out with an uncached DF
    ## (although this should already be the case).
    spark_df.unpersist()
    assert not spark_df.is_cached
    cached_df = renew_cache(spark_df)
    assert cached_df.is_cached
    recached_df = renew_cache(cached_df)
    assert recached_df.is_cached


def test_get_colname(spark_df):
    assert get_colname(spark_df["label"]) == "label"
    assert get_colname(spark_df["value"]) == "value"


def test_count_distinct(spark_df):
    ## With no columns specified, this should compute
    ## the number of distinct rows.
    assert count_distinct(spark_df) == 8

    ## Test the different ways of specifying columns.
    ## This is handled by the DF's select() method.
    assert count_distinct(spark_df, "label", "value") == 8
    assert count_distinct(spark_df, ["label", "value"]) == 8
    assert count_distinct(spark_df, spark_df["label"], spark_df["value"]) == 8

    assert count_distinct(spark_df, "label") == 3
    assert count_distinct(spark_df, "value") == 5

    df_a = spark_df.where("label = 'a'")
    assert count_distinct(df_a) == 3
    assert count_distinct(df_a, "label") == 1


def compare_csv(observed_csv_files, expected_rows):
    """Compare a CSV file or files written by Spark against the expected rows.

    Spark may not write rows in the original order. The file is as expected if
    the headers match and the same rows are present.
    """
    if not isinstance(observed_csv_files, (list, tuple)):
        observed_csv_files = [observed_csv_files]
    observed_csv_files = [str(f) for f in observed_csv_files]

    header_rows = []
    data_rows = []

    def read_csv_rows(fname):
        """Read a CSV file that is possibly gzipped."""
        if fname.endswith(".gz"):
            with gzip.open(fname) as f:
                contents = f.read().decode("utf-8")
                return contents.split()
        else:
            with open(fname) as f:
                rows = f.readlines()
                return [r.rstrip("\n") for r in rows]

    for fname in observed_csv_files:
        observed_rows = read_csv_rows(fname)
        header_rows.append(observed_rows.pop(0))
        data_rows.extend(observed_rows)
    ## Compare headers.
    ## They should all be the same as the main table header.
    header = set(header_rows)
    assert len(header) == 1
    assert header.pop() == expected_rows[0]
    ## Compare main rows.
    assert set(data_rows) == set(expected_rows[1:])


def test_dump_to_csv(spark_df, csv_rows, tmp_path):
    ## It would be best to also test writing to S3. However, it is difficult
    ## to mock because of AWS/Spark configuration issues.
    ## For now, we only test locally. Files written to S3 should have
    ## the same contents.
    def dump(path, **kwargs):
        dump_to_csv(spark_df, str(path), simplify=False, **kwargs)

    ## Single-part, uncompressed.
    df_path = tmp_path.joinpath("df")
    dump(df_path, compress=False)
    ## There should be a single CSV with a machine-generated name.
    csv_files = list(df_path.glob("*.csv"))
    assert len(csv_files) == 1
    compare_csv(csv_files, csv_rows)

    ## Multiple part files.
    df_path = tmp_path.joinpath("df2")
    dump(df_path, num_parts=4, compress=False)
    ## There should be 4 CSVs.
    csv_files = list(df_path.glob("*.csv"))
    assert len(csv_files) == 4
    compare_csv(csv_files, csv_rows)

    ## Single-part, compressed.
    df_path = tmp_path.joinpath("df3")
    dump(df_path, compress=True)
    ## There should be a single gzipped CSV with a machine-generated name.
    csv_files = list(df_path.glob("*.csv.gz"))
    assert len(csv_files) == 1
    compare_csv(csv_files, csv_rows)

    ## Write mode is just the one used by Spark's DataFrameWriter.
    ## Just test that the default throws an error on overwrite, and that
    ## overwriting works as expected.
    df_path = tmp_path.joinpath("df4")
    dump(df_path, compress=False)
    with pytest.raises(AnalysisException):
        dump(df_path, compress=False)

    dump_to_csv(spark_df.where("label = 'a'"),
                str(df_path),
                write_mode="overwrite",
                compress=False,
                simplify=False)
    csv_files = list(df_path.glob("*.csv"))
    assert len(csv_files) == 1
    a_rows = [r for i, r in enumerate(csv_rows) if i == 0 or r.startswith("a,")]
    compare_csv(csv_files, a_rows)


def dir_contains(contents, files):
    """Verify that a dir contains specific files."""
    contents = [p.name for p in contents]
    return set(contents) == set(files)


def test_simplify_local(spark_df, csv_rows, tmp_path, capsys):
    def dump(path, **kwargs):
        dump_to_csv(spark_df, str(path), simplify=True, **kwargs)

    def verify_single(out_path, prior_msg=""):
        assert (
            capsys.readouterr().out ==
            prior_msg + "CSV output was written to {}.\n".format(out_path)
        )
        assert out_path.is_file()
        compare_csv(out_path, csv_rows)

    def verify_multiple(out_path, expected_files):
        assert (
            capsys.readouterr().out ==
            "CSV output was written to {} across files {} to {}.\n".format(
                out_path,
                expected_files[0],
                expected_files[-1]
            )
        )
        assert out_path.is_dir()
        csvs = [p for p in out_path.glob("*") if p.name in expected_files]
        compare_csv(csvs, csv_rows)

    ## Single part file.
    df_path = tmp_path.joinpath("df1")
    dump(df_path, compress=False)
    new_df_path = tmp_path.joinpath("df1.csv")
    verify_single(new_df_path)

    ## When the new filename already exists.
    dump(df_path, compress=False)
    verify_single(df_path,
                  "Target file {} already exists.\n".format(new_df_path))

    ## When the output dirname has the extension.
    df_path = tmp_path.joinpath("df2.csv")
    dump(df_path, compress=False)
    new_df_path = tmp_path.joinpath("df2.csv")
    verify_single(new_df_path)

    ## Compressed.
    df_path = tmp_path.joinpath("df3")
    dump(df_path, compress=True)
    new_df_path = tmp_path.joinpath("df3.csv.gz")
    verify_single(new_df_path)

    ## When the new filename already exists.
    dump(df_path, compress=True)
    assert (
        capsys.readouterr().out ==
        "Target file {} already exists.\n".format(new_df_path) +
        "Gzipped CSV output was written to {}.\n".format(df_path)
    )
    assert df_path.is_file()
    ## Append the gzip extension so that it can be opened.
    gz_path = df_path.with_suffix(".gz")
    df_path.rename(gz_path)
    compare_csv(gz_path, csv_rows)

    ## When the output dirname has the extension.
    df_path = tmp_path.joinpath("df4.csv.gz")
    dump(df_path, compress=True)
    new_df_path = tmp_path.joinpath("df4.csv.gz")
    verify_single(new_df_path)

    ## When the output dirname has only the CSV extension.
    df_path = tmp_path.joinpath("df5.csv")
    dump(df_path, compress=True)
    new_df_path = tmp_path.joinpath("df5.csv.gz")
    verify_single(new_df_path)

    ## Multiple part files.
    df_path = tmp_path.joinpath("df6")
    dump(df_path, num_parts=3, compress=False)
    contents = list(df_path.glob("*"))
    assert dir_contains(contents, [
        "part1.csv",
        "part2.csv",
        "part3.csv"
    ])
    verify_multiple(df_path, ["part1.csv", "part2.csv", "part3.csv"])

    ## When files are already present.
    ## Modify the contents to:
    ## part1.csv, part3.csv, part4.csv.gz, part5_5.csv, other.txt.
    extra_file = df_path.joinpath("part2.csv")
    shutil.copy(str(extra_file), str(df_path.joinpath("other.txt")))
    shutil.copy(str(extra_file), str(df_path.joinpath("part4.csv.gz")))
    extra_file.rename(df_path.joinpath("part5-5.csv"))
    dump(df_path, num_parts=3, compress=False, write_mode="append")
    contents = list(df_path.glob("*"))
    assert dir_contains(contents, [
        "part1.csv",
        "part3.csv",
        "part4.csv.gz",
        "part5.csv",
        "part6.csv",
        "part7.csv",
        "part5-5.csv",
        "other.txt"
    ])
    verify_multiple(df_path, ["part5.csv", "part6.csv", "part7.csv"])

    ## Mutiple compressed part files.
    df_path = tmp_path.joinpath("df7")
    dump(df_path, num_parts=3, compress=True)
    contents = list(df_path.glob("*"))
    assert dir_contains(contents, [
        "part1.csv.gz",
        "part2.csv.gz",
        "part3.csv.gz"
    ])
    verify_multiple(df_path, ["part1.csv.gz", "part2.csv.gz", "part3.csv.gz"])

    ## When files are already present.
    ## Modify the contents to:
    ## part1.csv.gz, part3.csv.gz, part4.csv, part5_5.csv.gz, other.txt.
    extra_file = df_path.joinpath("part2.csv.gz")
    shutil.copy(str(extra_file), str(df_path.joinpath("other.txt")))
    shutil.copy(str(extra_file), str(df_path.joinpath("part4.csv")))
    extra_file.rename(df_path.joinpath("part5-5.csv.gz"))
    dump(df_path, num_parts=3, compress=True, write_mode="append")
    contents = list(df_path.glob("*"))
    assert dir_contains(contents, [
        "part1.csv.gz",
        "part3.csv.gz",
        "part4.csv",
        "part5.csv.gz",
        "part6.csv.gz",
        "part7.csv.gz",
        "part5-5.csv.gz",
        "other.txt"
    ])
    verify_multiple(df_path, ["part5.csv.gz", "part6.csv.gz", "part7.csv.gz"])


@mock_s3
def test_simplify_s3(spark_df, csv_rows, tmp_path, capsys):
    BUCKET_NAME = "test-bucket"
    S3 = boto3.resource("s3")
    S3.create_bucket(Bucket=BUCKET_NAME)
    BUCKET = S3.Bucket(BUCKET_NAME)

    def dump(prefix, **kwargs):
        ## To avoid problems connecting Spark to S3, just write files locally
        ## and push to S3 for testing.
        df_path = tmp_path.joinpath(prefix)
        dump_to_csv(spark_df, str(df_path), simplify=False, **kwargs)
        new_keys = []
        if not prefix.endswith("/"):
            prefix += "/"
        start_time = time.time()
        for p in df_path.glob("*"):
            key = str(p.relative_to(tmp_path))
            BUCKET.upload_file(str(p), key)
            new_keys.append(key)
        _simplify_csv_s3("s3://{}/{}".format(BUCKET_NAME, prefix), start_time)

    def verify_single(key, prior_msg=""):
        assert (
            capsys.readouterr().out ==
            prior_msg + "CSV output was written to {}.\n".format(key)
        )
        objects = list(BUCKET.objects.filter(Prefix=key))
        objects = [obj for obj in objects if obj.key == key]
        assert len(objects) == 1
        out_dir = tmp_path.joinpath(key + "_out")
        out_path = out_dir.joinpath(key)
        out_dir.mkdir(parents=True)
        BUCKET.download_file(key, str(out_path))
        compare_csv(out_path, csv_rows)

    def verify_multiple(prefix, all_contents, expected_files):
        if not prefix.endswith("/"):
            prefix += "/"
        assert (
            capsys.readouterr().out ==
            "CSV output was written to {} across files {} to {}.\n".format(
                prefix,
                expected_files[0],
                expected_files[-1]
            )
        )
        objects = list(BUCKET.objects.filter(Prefix=prefix))
        objects = [obj.key[len(prefix):] for obj in objects]
        assert set(objects) == set(all_contents)

        out_dir = tmp_path.joinpath(prefix + "_out")
        out_dir.mkdir(parents=True)
        for key in objects:
            if key in expected_files:
                BUCKET.download_file(prefix + key, str(out_dir.joinpath(key)))
        compare_csv([str(p) for p in out_dir.glob("*")], csv_rows)

    ## Single part file.
    prefix = "df1"
    dump(prefix, compress=False)
    new_key = "df1.csv"
    verify_single(new_key)

    ## When the new filename already exists.
    dump(prefix, compress=False, write_mode="overwrite")
    verify_single(prefix,
                  "Target key {} already exists.\n".format(new_key))

    ## When the output dirname has the extension.
    prefix = "df2.csv"
    dump(prefix, compress=False)
    new_key = "df2.csv"
    verify_single(new_key)

    ## Compressed.
    prefix = "df3"
    dump(prefix, compress=True)
    new_key = "df3.csv.gz"
    verify_single(new_key)

    ## When the new filename already exists.
    dump(prefix, compress=True, write_mode="overwrite")
    assert (
        capsys.readouterr().out ==
        "Target key {} already exists.\n".format(new_key) +
        "Gzipped CSV output was written to {}.\n".format(prefix)
    )
    objects = list(BUCKET.objects.filter(Prefix=prefix))
    objects = [obj for obj in objects if obj.key == prefix]
    assert len(objects) == 1
    out_dir = tmp_path.joinpath(prefix + "_out")
    ## Append the gzip extension so that it can be opened.
    out_path = out_dir.joinpath(prefix).with_suffix(".gz")
    out_dir.mkdir(parents=True)
    BUCKET.download_file(prefix, str(out_path))
    compare_csv(out_path, csv_rows)

    ## When the output dirname has the extension.
    prefix = "df4.csv.gz"
    dump(prefix, compress=True)
    new_key = "df4.csv.gz"
    verify_single(new_key)

    ## When the output dirname has only the CSV extension.
    prefix = "df5.csv"
    dump(prefix, compress=True)
    new_key = "df5.csv.gz"
    verify_single(new_key)

    ## Multiple part files.
    prefix = "df6"
    dump(prefix, num_parts=3, compress=False)
    verify_multiple(
        prefix,
        ["part1.csv", "part2.csv", "part3.csv"],
        ["part1.csv", "part2.csv", "part3.csv"]
    )

    ## When files are already present.
    copy_source = {"Bucket": BUCKET_NAME, "Key": prefix + "/part2.csv"}
    BUCKET.copy(copy_source, prefix + "/other.txt")
    BUCKET.copy(copy_source, prefix + "/part4.csv.gz")
    BUCKET.copy(copy_source, prefix + "/part5.csv/other.txt")
    BUCKET.copy(copy_source, prefix + "/part6-5.csv")
    BUCKET.delete_objects(Delete={"Objects": [{"Key": copy_source["Key"]}]})
    ## Use write_mode="overwrite" to overwrite the previous local output,
    ## but not the contents on S3.
    dump(prefix, num_parts=3, compress=False, write_mode="overwrite")
    verify_multiple(
        prefix,
        [
            "part1.csv",
            "part3.csv",
            "part4.csv.gz",
            "part5.csv/other.txt",
            "part6.csv",
            "part7.csv",
            "part8.csv",
            "part6-5.csv",
            "other.txt"
        ],
        ["part6.csv", "part7.csv", "part8.csv"]
    )

    ## Mutiple compressed part files.
    prefix = "df7"
    dump(prefix, num_parts=3, compress=True)
    verify_multiple(
        prefix,
        ["part1.csv.gz", "part2.csv.gz", "part3.csv.gz"],
        ["part1.csv.gz", "part2.csv.gz", "part3.csv.gz"]
    )

    ## When files are already present.
    copy_source = {"Bucket": BUCKET_NAME, "Key": prefix + "/part2.csv.gz"}
    BUCKET.copy(copy_source, prefix + "/other.txt")
    BUCKET.copy(copy_source, prefix + "/part4.csv")
    BUCKET.copy(copy_source, prefix + "/part5.csv/other.txt")
    BUCKET.copy(copy_source, prefix + "/part6-5.csv.gz")
    BUCKET.delete_objects(Delete={"Objects": [{"Key": copy_source["Key"]}]})
    ## Use write_mode="overwrite" to overwrite the previous local output,
    ## but not the contents on S3.
    dump(prefix, num_parts=3, compress=True, write_mode="overwrite")
    verify_multiple(
        prefix,
        [
            "part1.csv.gz",
            "part3.csv.gz",
            "part4.csv",
            "part5.csv/other.txt",
            "part6.csv.gz",
            "part7.csv.gz",
            "part8.csv.gz",
            "part6-5.csv.gz",
            "other.txt"
        ],
        ["part6.csv.gz", "part7.csv.gz", "part8.csv.gz"]
    )
