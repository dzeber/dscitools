import pytest
from pandas import DataFrame
from numpy import nan as npnan
from dscitools.pandas import (
    FormattedDataFrame,
    fmt_df
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


def compare_formatting(original, expected, **kwargs):
    ## Test both the string and HTML representations for explicit object
    ## creation and the API function.
    fmt_obj = FormattedDataFrame(original, **kwargs)
    fmt_api = fmt_df(original, **kwargs)
    assert fmt_obj.to_string() == expected.to_string()
    assert fmt_api.to_string() == expected.to_string()
    assert fmt_obj.to_html() == expected.to_html()
    assert fmt_api.to_html() == expected.to_html()


def dollar_fmt(v):
    ## Apply dollar formatting with the dollar sign in front
    ## of any minus signs.
    v_fmt = "{:,.2f}".format(v)
    if v_fmt.startswith("-"):
        v_fmt = v_fmt[0] + "$" + v_fmt[1:]
    else:
        v_fmt = "$" + v_fmt
    return v_fmt


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

