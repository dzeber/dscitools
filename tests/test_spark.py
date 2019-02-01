import pytest
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from pyspark.sql import SparkSession

from dscitools.spark import (
    show_df,
    renew_cache,
    get_colname,
    count_distinct
)


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession\
        .builder\
        .master("local")\
        .appName("dscitools_test")\
        .getOrCreate()
    yield spark

    ## Teardown
    spark.stop()


#@pytest.fixture(scope="module")
#def spark_context(spark):
#    return spark.sparkContext


@pytest.fixture
def pandas_df():
    return DataFrame({
        "label": ["a", "a", "a", "b", "b", "b", "c", "c"],
        "value": [1, 2, 3, 4, 5, 1, 2, 3]
    })


@pytest.fixture
def spark_df(spark, pandas_df):
    return spark.createDataFrame(pandas_df)


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
