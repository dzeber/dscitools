import re

import pytest
from pandas import DataFrame

from numpy import nan as npnan
from IPython.display import display

from dscitools.pandas import FormattedDataFrame, fmt_df, fmt_count_df
from tests.utils import get_notebook_rich_output


BASE_DF = {
    "group": ["a", "b", "c", "d", "e"],
    "num": [10235, 6426, 325, -9453, -1362],
    "prop": [0.02426, 0.3333333, 0.578135, 0.7, 0.25],
}


COUNT_DF = {"group": ["a", "b", "c"], "count": [1, 4, 3], "total": [8, 20, 12]}

N_OVERALL = 100


@pytest.fixture
def df():
    return DataFrame(BASE_DF)[["group", "num", "prop"]]


@pytest.fixture
def count_total_df():
    return DataFrame(COUNT_DF)[["group", "count", "total"]]


@pytest.fixture
def count_df():
    return DataFrame(COUNT_DF)[["group", "count"]]


class TestFormatting:
    """Test different formatting options for FormattedDataFrame.

    Outputs are compared as rendered both to string and to HTML in console and
    notebook contexts.
    """

    def _formatteddataframe_notebook_display(self, fdf):
        """Pull the rich output generated by `display`ing the FDF in a notebook.

        Returns (plaintext, html) outputs. HTML output is restricted to the
        `table` element.
        """
        data = get_notebook_rich_output(display, fdf)
        df_str = data.get("text/plain")
        # The notebook may inject additional styling, etc.
        # Pull out the `table` element (allowing for attrs in the opening tag).
        df_html_match = re.search(
            "<table.*</table>", data.get("text/html"), flags=re.DOTALL
        )
        assert df_html_match is not None
        return (df_str, df_html_match.group())

    def _compare_fdf_string_html(self, fdf, expected, formatters=None):
        """Test string and HMTL representations of a FormattedDataFrame.

        Compares both against the corresponding representations for an expected
        DF.

        The FormattedDataFrame is also rendered using both representations in a
        Jupyter notebook context, and the results are compared against the same
        expected versions. This is skipped if formatters are supplied
        explicitly, as they are not passed along to the notebook rendering call.

        Note that this assumes the notebook representation will match the
        expected `to_html()` result.
        """
        expected_str = expected.to_string()
        expected_html = expected.to_html()

        assert fdf.to_string(formatters=formatters) == expected_str
        assert fdf.to_html(formatters=formatters) == expected_html

        if not formatters:
            # Notebook formatting doesn't allow for setting `formatters`
            # explicitly.
            nb_str, nb_html = self._formatteddataframe_notebook_display(fdf)
            assert nb_str == expected.to_string()
            assert nb_html == expected.to_html()

    def compare_formatting(
        self, observed, expected, formatters=None, **kwargs
    ):
        """Test formatting for a DataFrame via FormattedDataFrame.

        This tests both explicit object instantiation as well as the API
        function. Comparisons are made in terms of both the string and HTML
        representations.

        If formatters are specified explicitly, these are passed along to the
        representation functions.

        Additional keywords args are passed to the FDF instantiation.
        """
        fdf_obj = FormattedDataFrame(observed, **kwargs)
        fdf_api = fmt_df(observed, **kwargs)
        self._compare_fdf_string_html(fdf_obj, expected, formatters=formatters)
        self._compare_fdf_string_html(fdf_api, expected, formatters=formatters)

    def test_default_formatting(self, df):
        expected = DataFrame(
            {
                "group": ["a", "b", "c", "d", "e"],
                "num": ["10,235", "6,426", "325", "-9,453", "-1,362"],
                "prop": [0.02, 0.33, 0.58, 0.70, 0.25],
            }
        )[["group", "num", "prop"]]

        self.compare_formatting(df, expected)

    def test_explicit_formatter_override(self, df):
        expected = DataFrame(
            {
                "group": ["a", "b", "c", "d", "e"],
                "num": ["10235.0", "6426.0", "325.0", "-9453.0", "-1362.0"],
                "prop": ["0.0243", "0.3333", "0.5781", "0.7000", "0.2500"],
            }
        )[["group", "num", "prop"]]

        explicit_fmt = {
            # This should override dynamic detection of "num" as an int column.
            "num": "{: .1f}".format,
            # This should override static formatting of "prop" as a pct column.
            "prop": "{: .4f}".format,
        }

        self.compare_formatting(
            df, expected, formatters=explicit_fmt, fmt_percent="prop"
        )

    def test_static_formatting(self, df):
        expected = DataFrame(
            {
                "group": ["a", "b", "c", "d", "e"],
                "num": ["10,235", "6,426", "325", "-9,453", "-1,362"],
                "prop": [0.02426, 0.3333333, 0.578135, 0.7, 0.25],
                "numfl": [
                    "10,235.00",
                    "6,426.00",
                    "325.00",
                    "-9,453.00",
                    "-1,362.00",
                ],
                "amount": [
                    "$10,235.00",
                    "$6,426.00",
                    "$325.00",
                    "-$9,453.00",
                    "-$1,362.00",
                ],
                "pct": ["2.43%", "33.33%", "57.81%", "70.00%", "25.00%"],
            }
        )[["group", "num", "prop", "numfl", "amount", "pct"]]

        df["numfl"] = df["num"]
        df["amount"] = df["num"]
        df["pct"] = df["prop"]

        self.compare_formatting(
            df,
            expected,
            # Override automatic int formatting for 'numfl'.
            # This also deactivates float column detection.
            fmt_float="numfl",
            fmt_percent="pct",
            fmt_dollar="amount",
        )

    def test_fmt_custom_precision(self, df):
        expected = DataFrame(
            {
                "group": ["a", "b", "c", "d", "e"],
                "num": ["10,235", "6,426", "325", "-9,453", "-1,362"],
                "prop": ["0.0243", "0.3333", "0.5781", "0.7000", "0.2500"],
                "pct": ["2.426%", "33.333%", "57.813%", "70.000%", "25.000%"],
            }
        )[["group", "num", "prop", "pct"]]

        df["pct"] = df["prop"]

        self.compare_formatting(
            df, expected, fmt_percent="pct", precision={"prop": 4, "pct": 3}
        )

    def test_modified_formatteddataframe(self, df):
        # TODO
        expected = df.copy()
        expected["num"] = expected["num"].apply("{:,}".format)
        expected["prop"] = expected["prop"].apply("{:.2f}".format)

        # Subsetting a FDF.
        fdf = FormattedDataFrame(df)
        assert fdf[["prop"]].to_string() == expected[["prop"]].to_string()
        assert fdf[["prop"]].to_html() == expected[["prop"]].to_html()
        assert fdf[:3].to_string() == expected[:3].to_string()
        assert fdf[:3].to_html() == expected[:3].to_html()

        # Adding a column to a FDF with no static formatting.
        fdf2 = fdf.copy()
        fdf2["prop2"] = fdf2["prop"]
        expected["prop2"] = df["prop"].apply("{:.2f}".format)
        assert fdf2.to_string() == expected.to_string()
        assert fdf2.to_html() == expected.to_html()

        # Adding a column to a FDF with static formatting.
        fdf3 = FormattedDataFrame(df, fmt_percent="prop")
        fdf3["prop2"] = fdf3["prop"]
        expected["prop"] = df["prop"].apply("{:.2%}".format)
        with pytest.warns(UserWarning):
            assert fdf3.to_string() == expected.to_string()
        with pytest.warns(UserWarning):
            assert fdf3.to_html() == expected.to_html()

    def test_fmt_args(self, df):
        expected = DataFrame(
            {
                "group": ["a", "b", "c", "d", "e"],
                "num": ["10,235", "6,426", "325", "-9,453", "-1,362"],
                "prop": ["0.02", "0.33", "0.58", "0.70", "0.25"],
                "numfl": [
                    "10,235.00",
                    "6,426.00",
                    "325.00",
                    "-9,453.00",
                    "-1,362.00",
                ],
                "pct": ["2.43%", "33.33%", "57.81%", "70.00%", "25.00%"],
            }
        )[["group", "num", "prop", "numfl", "pct"]]

        df["numfl"] = df["num"]
        df["pct"] = df["prop"]

        self.compare_formatting(
            df,
            expected,
            # Formatting params should accept both single strings and list-likes.
            fmt_int="num",
            fmt_float=["numfl", "prop"],
            fmt_percent={"pct"},
            # This should get silently ignored.
            fmt_dollar=4,
        )

    def test_fmt_na_handling(self, df):
        expected = DataFrame(
            {
                "group": ["a", "b", "c", "d", "e"],
                "num": ["10,235", "NaN", "325", "-9,453", "-1,362"],
                "prop": ["0.02", "0.33", "0.58", "NaN", "0.25"],
            }
        )[["group", "num", "prop"]]

        df.loc[1, "num"] = npnan
        df.loc[3, "prop"] = npnan

        # Formatting should work as if NaNs were excluded.
        self.compare_formatting(df, expected)

        expected = DataFrame(
            {
                "group": ["a", "b", "c", "d", "e"],
                "num": [
                    "$10,235.00",
                    "NaN",
                    "$325.00",
                    "-$9,453.00",
                    "-$1,362.00",
                ],
                "prop": ["2.426%", "33.333%", "57.813%", "NaN", "25.000%"],
            }
        )[["group", "num", "prop"]]

        self.compare_formatting(
            df,
            expected,
            fmt_percent="prop",
            fmt_dollar="num",
            precision={"prop": 3},
        )


class TestFmtCountDF:
    """Test the added columns and computations associated with `fmt_count_df`.

    This mainly covers the different ways args can be passed.

    Ignore the final formatting step for these tests as that is covered above.
    """

    def compare_count_df(self, expected, *args, **kwargs):
        """Skip formatting and test for exact equality.

        Best used when floats are nice and round.
        """
        result = fmt_count_df(*args, fmt=False, **kwargs)
        assert result.equals(expected)

    def test_fmt_count_default(self, count_df, count_total_df):
        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "total": [8, 20, 12],
                "proportion": [0.125, 0.5, 0.375],
                "prop_total": [0.2, 0.5, 0.3],
            }
        )[["group", "count", "total", "proportion", "prop_total"]]

        # Test the default behaviour when no switches are used.
        self.compare_count_df(
            expected[["group", "count", "proportion"]], count_df
        )

        # Multiple count columns.
        self.compare_count_df(
            expected, count_total_df, count_col=["count", "total"]
        )

    def test_fmt_count_n_overall(self, count_df, count_total_df):
        # Case when n_overall is a number:
        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "proportion": [0.01, 0.04, 0.03],
            }
        )[["group", "count", "proportion"]]

        self.compare_count_df(expected, count_df, n_overall=N_OVERALL)

        # Case when n_overall is a column.
        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "total": [8, 20, 12],
                "proportion": [0.125, 0.2, 0.25],
            }
        )[["group", "count", "total", "proportion"]]

        self.compare_count_df(expected, count_total_df, n_overall="total")
        self.compare_count_df(expected, count_total_df, n_overall=["total"])

        # Case when n_overall is a list.
        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "total": [8, 20, 12],
                "proportion": [0.125, 0.5, 0.375],
                "prop_total": [0.08, 0.2, 0.12],
            }
        )[["group", "count", "total", "proportion", "prop_total"]]

        self.compare_count_df(
            expected,
            count_total_df,
            n_overall=[None, N_OVERALL],
            count_col=["count", "total"],
        )

        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "total": [8, 20, 12],
                "proportion": [0.125, 0.2, 0.25],
                "prop_total": [0.2, 0.5, 0.3],
            }
        )[["group", "count", "total", "proportion", "prop_total"]]

        self.compare_count_df(
            expected,
            count_total_df,
            n_overall=["total", None],
            count_col=["count", "total"],
        )

        # Reversing the count column order should reverse the order in which
        # the proportion columns appear in the result, with the n_overall list
        # elements applied to the count column names in the same order.
        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "total": [8, 20, 12],
                "prop_total": [0.08, 0.2, 0.12],
                "proportion": [0.125, 0.2, 0.25],
            }
        )[["group", "count", "total", "prop_total", "proportion"]]

        self.compare_count_df(
            expected,
            count_total_df,
            n_overall=[N_OVERALL, "total"],
            count_col=["total", "count"],
        )

    def test_fmt_count_colnames(self, count_total_df):
        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "total": [8, 20, 12],
                "prop_total": [0.2, 0.5, 0.3],
            }
        )[["group", "count", "total", "prop_total"]]

        self.compare_count_df(expected, count_total_df, count_col="total")
        self.compare_count_df(expected, count_total_df, count_col=["total"])

        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "total": [8, 20, 12],
                "prop_total": [0.2, 0.5, 0.3],
                "proportion": [0.125, 0.5, 0.375],
            }
        )[["group", "count", "total", "prop_total", "proportion"]]

        self.compare_count_df(
            expected, count_total_df, count_col=["total", "count"]
        )

    def test_fmt_count_naming_schemes(self, count_df):
        # Test automatic detection of count columns by naming scheme
        # and corresponding automatic naming of proportion columns.
        count_df["n"] = count_df["count"]
        count_df["num"] = count_df["count"]
        count_df["n_items"] = count_df["count"]
        count_df["num_items"] = count_df["count"]

        expected = DataFrame(
            {
                "group": ["a", "b", "c"],
                "count": [1, 4, 3],
                "n": [1, 4, 3],
                "num": [1, 4, 3],
                "n_items": [1, 4, 3],
                "num_items": [1, 4, 3],
                "proportion": [0.125, 0.5, 0.375],
                "p": [0.125, 0.5, 0.375],
                "prop": [0.125, 0.5, 0.375],
                "p_items": [0.125, 0.5, 0.375],
                "prop_items": [0.125, 0.5, 0.375],
            }
        )[
            [
                "group",
                "count",
                "n",
                "num",
                "n_items",
                "num_items",
                "proportion",
                "p",
                "prop",
                "p_items",
                "prop_items",
            ]
        ]

        self.compare_count_df(expected, count_df)


@pytest.mark.skip
def test_fmt_count_col_sort(count_df, count_total_df, prop_df, prop_total_df):
    # Test sorting by count column.
    prop_df = prop_df.sort_values("count", ascending=False)
    result = fmt_count_df(count_df, order_by_count=True, fmt=False)
    assert result.equals(prop_df)
    result = fmt_count_df(count_df, order_by_count="count", fmt=False)
    assert result.equals(prop_df)
    result = fmt_count_df(count_df, order_by_count=["count"], fmt=False)
    assert result.equals(prop_df)

    prop_total_df = prop_total_df.sort_values(
        ["total", "count"], ascending=False
    )
    result = fmt_count_df(
        count_total_df,
        count_col=["count", "total"],
        order_by_count=["total", "count"],
        fmt=False,
    )
    assert result.equals(prop_total_df)
    # Sorting should use the first count column when unspecified.
    expected = prop_total_df[
        ["group", "count", "total", "prop_total", "proportion"]
    ]
    result = fmt_count_df(
        count_total_df,
        count_col=["total", "count"],
        order_by_count=True,
        fmt=False,
    )
    assert result.equals(expected)


@pytest.mark.skip
def test_fmt_count_cum_pct(count_df, count_total_df, prop_df, prop_total_df):
    expected_df = DataFrame(
        {
            "group": ["a", "b", "c"],
            "count": ["10,235", "325", "6,426"],
            "total": ["15,590", "498", "15,593"],
            "proportion": ["60.26%", "1.91%", "37.83%"],
            "prop_total": ["49.21%", "1.57%", "49.22%"],
            "cum proportion": ["60.26%", "62.17%", "100.00%"],
            "cum prop_total": ["49.21%", "50.78%", "100.00%"],
        }
    )[
        [
            "group",
            "count",
            "total",
            "proportion",
            "prop_total",
            "cum proportion",
        ]
    ]

    # Test appending cumulative percentages.
    expected = expected_df[["group", "count", "proportion", "cum proportion"]]
    result = fmt_count_df(count_df, show_cum_pct=True, fmt=False)
    compare_fdf_string_html(result, expected)
    result = fmt_count_df(count_df, show_cum_pct="count", fmt=False)
    compare_fdf_string_html(result, expected)
    result = fmt_count_df(count_df, show_cum_pct=["count"], fmt=False)
    compare_fdf_string_html(result, expected)

    expected = expected_df
    result = fmt_count_df(
        count_total_df,
        count_col=["count", "total"],
        show_cum_pct=True,
        fmt=False,
    )
    compare_fdf_string_html(result, expected)
    result = fmt_count_df(
        count_total_df,
        count_col=["count", "total"],
        show_cum_pct=["count", "total"],
        fmt=False,
    )
    compare_fdf_string_html(result, expected)
    # Ordering of the cumulative proportion columns follows
    # count column ordering.
    result = fmt_count_df(
        count_total_df,
        count_col=["count", "total"],
        show_cum_pct=["total", "count"],
        fmt=False,
    )
    compare_fdf_string_html(result, expected)

    # Cumulative prop for a single count column only.
    expected = expected_df.drop(columns="cum proportion")
    result = fmt_count_df(
        count_total_df,
        count_col=["count", "total"],
        show_cum_pct="total",
        fmt=False,
    )
    compare_fdf_string_html(result, expected)


@pytest.mark.skip
def test_fmt_count_pct_colnames(count_df, count_total_df):
    expected_df = DataFrame(
        {
            "group": ["a", "b", "c"],
            "count": ["10,235", "325", "6,426"],
            "total": ["15,590", "498", "15,593"],
            "thepct": ["60.26%", "1.91%", "37.83%"],
            "prop_total": ["49.21%", "1.57%", "49.22%"],
        }
    )[["group", "count", "total", "thepct", "prop_total"]]

    # Test supplying custom names for the percent columns.
    expected = expected_df[["group", "count", "thepct"]]
    result = fmt_count_df(count_df, pct_col_name="thepct", fmt=False)
    compare_fdf_string_html(result, expected)
    result = fmt_count_df(count_df, pct_col_name=["thepct"], fmt=False)
    compare_fdf_string_html(result, expected)

    # When there are multiple count columns
    expected = expected_df
    result = fmt_count_df(
        count_total_df,
        count_col=["count", "total"],
        pct_col_name=["thepct", None],
        fmt=False,
    )
    compare_fdf_string_html(result, expected)

    expected = expected_df.rename(columns={"prop_total": "totpct"})
    result = fmt_count_df(
        count_total_df,
        count_col=["count", "total"],
        pct_col_name=["thepct", "totpct"],
        fmt=False,
    )
    compare_fdf_string_html(result, expected)


@pytest.mark.skip
def test_fmt_count_fmt_precision(count_df):
    expected = DataFrame(
        {
            "group": ["a", "b", "c"],
            "count": ["10,235", "325", "6,426"],
            "proportion": ["60.26%", "1.91%", "37.83%"],
        }
    )[["group", "count", "proportion"]]

    # Test the conversion to FormattedDataFrame.
    result = fmt_count_df(count_df)
    compare_fdf_string_html(result, expected)

    # Custom precision for percent columns.
    expected["proportion"] = ["60.2555%", "1.9133%", "37.8312%"]
    result = fmt_count_df(count_df, pct_precision=4)
    compare_fdf_string_html(result, expected)
