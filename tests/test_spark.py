import pytest
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal
import gzip
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from dscitools.spark import (
    show_df,
    renew_cache,
    get_colname,
    count_distinct,
    dump_to_csv
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

    ## Single-part, uncompressed.
    df_path = tmp_path.joinpath("df")
    dump_to_csv(spark_df, str(df_path), compress=False)
    ## There should be a single CSV with a machine-generated name.
    csv_files = list(df_path.glob("*.csv"))
    assert len(csv_files) == 1
    compare_csv(csv_files, csv_rows)

    ## Multiple part files.
    df_path = tmp_path.joinpath("df2")
    dump_to_csv(spark_df, str(df_path), num_parts=4, compress=False)
    ## There should be 4 CSVs.
    csv_files = list(df_path.glob("*.csv"))
    assert len(csv_files) == 4
    compare_csv(csv_files, csv_rows)

    ## Single-part, compressed.
    df_path = tmp_path.joinpath("df3")
    dump_to_csv(spark_df, str(df_path), compress=True)
    ## There should be a single gzipped CSV with a machine-generated name.
    csv_files = list(df_path.glob("*.csv.gz"))
    assert len(csv_files) == 1
    compare_csv(csv_files, csv_rows)

    ## Write mode is just the one used by Spark's DataFrameWriter.
    ## Just test that the default throws an error on overwrite, and that
    ## overwriting works as expected.
    df_path = tmp_path.joinpath("df4")
    dump_to_csv(spark_df, str(df_path), compress=False)
    with pytest.raises(AnalysisException):
        dump_to_csv(spark_df, str(df_path), compress=False)

    dump_to_csv(spark_df.where("label = 'a'"),
                str(df_path),
                write_mode="overwrite",
                compress=False)
    csv_files = list(df_path.glob("*.csv"))
    assert len(csv_files) == 1
    a_rows = [r for i, r in enumerate(csv_rows) if i == 0 or r.startswith("a,")]
    compare_csv(csv_files, a_rows)
