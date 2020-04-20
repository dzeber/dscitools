import pytest
from pandas import DataFrame
from numpy import nan as npnan
from dscitools.pandas import (
    FormattedDataFrame,
    fmt_df,
    fmt_count_df
)


@pytest.fixture
def df():
    df = DataFrame({
        "group": ["a", "b", "c", "d", "e"],
        "num": [10235, 6426, 325, -9453, -1362],
        "prop": [0.02426, 0.3333333, 0.572135, 0.7, 0.25]
    })
    df = df[["group", "num", "prop"]]
    return df


@pytest.fixture
def count_total_df():
    df = DataFrame({
        "group": ["a", "b", "c"],
        "count": [10235, 325, 6426],
        "total": [15590, 498, 15593]
    })
    df = df[["group", "count", "total"]]
    return df


@pytest.fixture
def count_df(count_total_df):
    return count_total_df[["group", "count"]]


@pytest.fixture
def prop_total_df(count_total_df):
    df = count_total_df.copy()
    df["proportion"] = df["count"] / df["count"].sum()
    df["prop_total"] = df["total"] / df["total"].sum()
    df = df[["group", "count", "total", "proportion", "prop_total"]]
    return df


@pytest.fixture
def prop_df(prop_total_df):
    return prop_total_df[["group", "count", "proportion"]]


@pytest.fixture
def n_overall():
    return 20000


def compare_formatteddataframe_string_html(fdf, expected):
    assert fdf.to_string() == expected.to_string()
    assert fdf.to_html() == expected.to_html()


def compare_formatting(original, expected, **kwargs):
    ## Test both the string and HTML representations for explicit object
    ## creation and the API function.
    fmt_obj = FormattedDataFrame(original, **kwargs)
    fmt_api = fmt_df(original, **kwargs)
    compare_formatteddataframe_string_html(fmt_obj, expected)
    compare_formatteddataframe_string_html(fmt_api, expected)


def dollar_fmt(v):
    ## Apply dollar formatting with the dollar sign in front
    ## of any minus signs.
    v_fmt = "{:,.2f}".format(v)
    if v_fmt.startswith("-"):
        v_fmt = v_fmt[0] + "$" + v_fmt[1:]
    else:
        v_fmt = "$" + v_fmt
    return v_fmt


### Testing for fmt_df ###

def test_default_formatting(df):
    expected = df.copy()
    expected["num"] = expected["num"].apply("{:,}".format)
    expected["prop"] = expected["prop"].apply("{:.2f}".format)
    compare_formatting(df, expected)


def test_explicit_formatter_override(df):
    expected = df.copy()
    expected["num"] = expected["num"].apply("{:.1f}".format)
    expected["prop"] = expected["prop"].apply("{:.4f}".format)

    fmt_obj = FormattedDataFrame(df, fmt_percent="prop")
    fmt_api = fmt_df(df, fmt_percent="prop")
    ## Spacing for positive/negative signs must be specified explicitly here.
    explicit_fmt = {
        ## This should override dynamic detection of "num" as an int column.
        "num": "{: .1f}".format,
        ## This should override static formatting of "prop" as a pct column.
        "prop": "{: .4f}".format
    }
    assert fmt_obj.to_string(formatters=explicit_fmt) == \
        expected.to_string()
    assert fmt_api.to_string(formatters=explicit_fmt) == \
        expected.to_string()
    assert fmt_obj.to_html(formatters=explicit_fmt) == \
        expected.to_html()
    assert fmt_api.to_html(formatters=explicit_fmt) == \
        expected.to_html()


def test_static_formatting(df):
    df["numfl"] = df["num"]
    df["amount"] = df["num"]
    df["pct"] = df["prop"]

    expected = df.copy()
    expected["num"] = expected["num"].apply("{:,}".format)
    expected["pct"] = expected["pct"].apply("{:.2%}".format)
    expected["numfl"] = expected["numfl"].apply("{:,.2f}".format)
    expected["amount"] = expected["amount"].apply(dollar_fmt)

    compare_formatting(df,
                       expected,
                       ## Override automatic int formatting for 'numfl'.
                       ## This also deactivates float column detection.
                       fmt_float="numfl",
                       fmt_percent="pct",
                       fmt_dollar="amount")


def test_fmt_custom_precision(df):
    df["pct"] = df["prop"]

    expected = df.copy()
    expected["num"] = expected["num"].apply("{:,}".format)
    expected["prop"] = expected["prop"].apply("{:.4f}".format)
    expected["pct"] = expected["pct"].apply("{:.3%}".format)

    compare_formatting(df,
                       expected,
                       fmt_percent="pct",
                       precision={"prop": 4, "pct": 3})


def test_modified_formatteddataframe(df):
    expected = df.copy()
    expected["num"] = expected["num"].apply("{:,}".format)
    expected["prop"] = expected["prop"].apply("{:.2f}".format)

    ## Subsetting a FDF.
    fdf = FormattedDataFrame(df)
    assert fdf[["prop"]].to_string() == expected[["prop"]].to_string()
    assert fdf[["prop"]].to_html() == expected[["prop"]].to_html()
    assert fdf[:3].to_string() == expected[:3].to_string()
    assert fdf[:3].to_html() == expected[:3].to_html()

    ## Adding a column to a FDF with no static formatting.
    fdf2 = fdf.copy()
    fdf2["prop2"] = fdf2["prop"]
    expected["prop2"] = df["prop"].apply("{:.2f}".format)
    assert fdf2.to_string() == expected.to_string()
    assert fdf2.to_html() == expected.to_html()

    ## Adding a column to a FDF with static formatting.
    fdf3 = FormattedDataFrame(df, fmt_percent="prop")
    fdf3["prop2"] = fdf3["prop"]
    expected["prop"] = df["prop"].apply("{:.2%}".format)
    with pytest.warns(UserWarning):
        assert fdf3.to_string() == expected.to_string()
    with pytest.warns(UserWarning):
        assert fdf3.to_html() == expected.to_html()


def test_fmt_args(df):
    ## Formatting params should accept both single strings and list-likes.
    df["numfl"] = df["num"]
    df["pct"] = df["prop"]

    expected = df.copy()
    expected["num"] = expected["num"].apply("{:,}".format)
    expected["prop"] = expected["prop"].apply("{:.2f}".format)
    expected["numfl"] = expected["numfl"].apply("{:,.2f}".format)
    expected["pct"] = expected["pct"].apply("{:.2%}".format)

    compare_formatting(df,
                       expected,
                       fmt_int="num",
                       fmt_float=["numfl", "prop"],
                       fmt_percent={"pct"},
                       ## This should get silently ignored.
                       fmt_dollar=4)


def test_fmt_na_handling(df):
    base_df = df
    df = base_df.copy()
    df.loc[1, "num"] = npnan
    df.loc[3, "prop"] = npnan

    ## Formatting should work as if NaNs were excluded.
    expected = base_df.copy()
    expected["num"] = expected["num"].apply("{:,}".format)
    expected["prop"] = expected["prop"].apply("{:.2f}".format)
    expected.loc[1, "num"] = FormattedDataFrame.NAN_STRING
    expected.loc[3, "prop"] = FormattedDataFrame.NAN_STRING
    compare_formatting(df, expected)

    expected = base_df.copy()
    expected["num"] = expected["num"].apply(dollar_fmt)
    expected["prop"] = expected["prop"].apply("{:.3%}".format)
    expected.loc[1, "num"] = FormattedDataFrame.NAN_STRING
    expected.loc[3, "prop"] = FormattedDataFrame.NAN_STRING
    compare_formatting(df,
                       expected,
                       fmt_percent="prop",
                       fmt_dollar="num",
                       precision={"prop": 3})


### Testing for fmt_count_df ###

## This mainly covers the different ways args can be passed.

def test_fmt_count_default(count_df,
                           prop_df,
                           count_total_df,
                           prop_total_df):
    ## Test the default behaviour when no switches are used.
    result = fmt_count_df(count_df, fmt=False)
    assert result.equals(prop_df)

    ## Multiple count columns.
    result = fmt_count_df(count_total_df,
                          count_col=["count", "total"],
                          fmt=False)
    assert result.equals(prop_total_df)


def test_fmt_count_n_overall(count_df,
                             count_total_df,
                             prop_df,
                             prop_total_df,
                             n_overall):
    ## Default case is covered in test_fmt_count_default().
    ## Case when n_overall is a number:
    expected = prop_df.copy()
    expected["proportion"] = expected["count"] / n_overall
    result = fmt_count_df(count_df,
                          n_overall=n_overall,
                          fmt=False)
    assert result.equals(expected)

    ## Case when n_overall is a column.
    expected = prop_total_df.copy()
    expected = expected.drop("prop_total", axis="columns")
    expected["proportion"] = expected["count"] / expected["total"]
    result = fmt_count_df(count_total_df,
                          n_overall="total",
                          fmt=False)
    assert result.equals(expected)
    result = fmt_count_df(count_total_df,
                          n_overall=["total"],
                          fmt=False)
    assert result.equals(expected)

    ## Case when n_overall is a list.
    expected = prop_total_df.copy()
    expected["prop_total"] = expected["total"] / n_overall
    result = fmt_count_df(count_total_df,
                          n_overall=[None, n_overall],
                          count_col=["count", "total"],
                          fmt=False)
    assert result.equals(expected)

    expected = prop_total_df.copy()
    expected["proportion"] = expected["count"] / expected["total"]
    result = fmt_count_df(count_total_df,
                          n_overall=["total", None],
                          count_col=["count", "total"],
                          fmt=False)
    assert result.equals(expected)

    ## Reversing the count column order should reverse the order in which
    ## the proportion columns appear in the result, with the n_overall list
    ## elements applied to the count column names in the same order.
    expected = prop_total_df.copy()
    expected["proportion"] = expected["count"] / expected["total"]
    expected["prop_total"] = expected["total"] / n_overall
    expected = expected[
        ["group", "count", "total", "prop_total", "proportion"]
    ]
    result = fmt_count_df(count_total_df,
                          n_overall=[n_overall, "total"],
                          count_col=["total", "count"],
                          fmt=False)
    assert result.equals(expected)


def test_fmt_count_colnames(count_total_df, prop_total_df):
    result = fmt_count_df(count_total_df,
                          count_col="total",
                          fmt=False)
    expected = prop_total_df[["group", "count", "total", "prop_total"]]
    assert result.equals(expected)
    result = fmt_count_df(count_total_df,
                          count_col=["total"],
                          fmt=False)
    assert result.equals(expected)
    ## The order of the proportion columns should follow the order of
    ## the count colunms.
    expected = prop_total_df[
        ["group", "count", "total", "prop_total", "proportion"]
    ]
    result = fmt_count_df(count_total_df,
                          count_col=["total", "count"],
                          fmt=False)
    assert result.equals(expected)


def test_fmt_count_naming_schemes(count_df, prop_df):
    ## Test automatic detection of count columns by naming scheme
    ## and corresponding automatic naming of proportion columns.
    count_df["n"] = count_df["count"]
    count_df["num"] = count_df["count"]
    count_df["n_items"] = count_df["count"]
    count_df["num_items"] = count_df["count"]

    prop_df[["n", "num", "n_items", "num_items"]] = count_df[
        ["n", "num", "n_items", "num_items"]
    ]
    prop_df["p"] = prop_df["proportion"]
    prop_df["prop"] = prop_df["proportion"]
    prop_df["p_items"] = prop_df["proportion"]
    prop_df["prop_items"] = prop_df["proportion"]
    prop_df = prop_df[["group", "count", "n", "num", "n_items", "num_items",
                       "proportion", "p", "prop", "p_items", "prop_items"]]

    result = fmt_count_df(count_df, fmt=False)
    assert result.equals(prop_df)


def test_fmt_count_col_sort(count_df,
                            count_total_df,
                            prop_df,
                            prop_total_df):
    ## Test sorting by count column.
    prop_df = prop_df.sort_values("count", ascending=False)
    result = fmt_count_df(count_df,
                          order_by_count=True,
                          fmt=False)
    assert result.equals(prop_df)
    result = fmt_count_df(count_df,
                          order_by_count="count",
                          fmt=False)
    assert result.equals(prop_df)
    result = fmt_count_df(count_df,
                          order_by_count=["count"],
                          fmt=False)
    assert result.equals(prop_df)

    prop_total_df = prop_total_df.sort_values(["total", "count"],
                                              ascending=False)
    result = fmt_count_df(count_total_df,
                          count_col=["count", "total"],
                          order_by_count=["total", "count"],
                          fmt=False)
    assert result.equals(prop_total_df)
    ## Sorting should use the first count column when unspecified.
    expected = prop_total_df[
        ["group", "count", "total", "prop_total", "proportion"]
    ]
    result = fmt_count_df(count_total_df,
                          count_col=["total", "count"],
                          order_by_count=True,
                          fmt=False)
    assert result.equals(expected)


def test_fmt_count_cum_pct(count_df,
                           count_total_df,
                           prop_df,
                           prop_total_df):
    ## Test appending cumulative percentages.
    prop_df["cum proportion"] = prop_df["proportion"].cumsum()
    result = fmt_count_df(count_df,
                          show_cum_pct=True,
                          fmt=False)
    assert result.equals(prop_df)
    result = fmt_count_df(count_df,
                          show_cum_pct="count",
                          fmt=False)
    assert result.equals(prop_df)
    result = fmt_count_df(count_df,
                          show_cum_pct=["count"],
                          fmt=False)
    assert result.equals(prop_df)

    prop_total_df["cum proportion"] = prop_total_df["proportion"].cumsum()
    prop_total_df["cum prop_total"] = prop_total_df["prop_total"].cumsum()
    result = fmt_count_df(count_total_df,
                          count_col=["count", "total"],
                          show_cum_pct=True,
                          fmt=False)
    assert result.equals(prop_total_df)
    result = fmt_count_df(count_total_df,
                          count_col=["count", "total"],
                          show_cum_pct=["count", "total"],
                          fmt=False)
    assert result.equals(prop_total_df)
    ## Ordering of the cumulative proportion columns follows
    ## count column ordering.
    result = fmt_count_df(count_total_df,
                          count_col=["count", "total"],
                          show_cum_pct=["total", "count"],
                          fmt=False)
    assert result.equals(prop_total_df)
    ## Cumulative prop for a single count column only.
    result = fmt_count_df(count_total_df,
                          count_col=["count", "total"],
                          show_cum_pct="total",
                          fmt=False)
    assert result.equals(prop_total_df.drop("cum proportion", axis="columns"))


def test_fmt_count_pct_colnames(count_df,
                                count_total_df,
                                prop_df,
                                prop_total_df):
    ## Test supplying custom names for the percent columns.
    prop_df = prop_df.rename(columns={"proportion": "thepct"})
    result = fmt_count_df(count_df,
                          pct_col_name="thepct",
                          fmt=False)
    assert result.equals(prop_df)
    result = fmt_count_df(count_df,
                          pct_col_name=["thepct"],
                          fmt=False)
    assert result.equals(prop_df)

    expected = prop_total_df.rename(columns={"proportion": "thepct"})
    result = fmt_count_df(count_total_df,
                          count_col=["count", "total"],
                          pct_col_name=["thepct", None],
                          fmt=False)
    assert result.equals(expected)
    expected = prop_total_df.rename(columns={"proportion": "thepct",
                                     "prop_total": "totpct"})
    result = fmt_count_df(count_total_df,
                          count_col=["count", "total"],
                          pct_col_name=["thepct", "totpct"],
                          fmt=False)
    assert result.equals(expected)


def test_fmt_count_fmt_precision(count_df, prop_df):
    ## Test the conversion to FormattedDataFrame.
    expected = prop_df.copy()
    expected["count"] = expected["count"].apply("{:,}".format)
    expected["proportion"] = expected["proportion"].apply("{:.2%}".format)
    result = fmt_count_df(count_df)
    compare_formatteddataframe_string_html(result, expected)

    ## Custom precision for percent columns.
    expected = prop_df.copy()
    expected["count"] = expected["count"].apply("{:,}".format)
    expected["proportion"] = expected["proportion"].apply("{:.4%}".format)
    result = fmt_count_df(count_df,
                          pct_precision=4)
    compare_formatteddataframe_string_html(result, expected)
