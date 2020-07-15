from dscitools import figsize, interval_breaks, x_comma_fmt, y_comma_fmt

import pytest
from numpy import allclose
from pandas import DataFrame
import plotnine as gg


LARGENUM_DF = {
    "x": [3937, 170, 2744, 1224, 639],
    "y": [3961, 2014, 1977, 70, -2659],
}


@pytest.fixture
def largenum_df():
    return DataFrame(LARGENUM_DF)


def validate_figure_size(th, w, h):
    # Check that `th` is a theme object with figure_size set
    # and that the dimensions are correct.
    assert "figure_size" in th.themeables
    th_w, th_h = th.themeables["figure_size"].properties["value"]
    assert th_w == w
    assert th_h == h


def test_figsize():
    validate_figure_size(figsize(), 8, 4.936)
    validate_figure_size(figsize(ratio=1), 8, 8)
    validate_figure_size(figsize(width=4), 4, 2.468)
    validate_figure_size(figsize(width=4, ratio=1), 4, 4)
    validate_figure_size(figsize(width=4, height=7), 4, 7)
    validate_figure_size(figsize(width=4, height=7, ratio=1), 4, 7)
    validate_figure_size(figsize(height=4), 6.48, 4)
    validate_figure_size(figsize(height=4, ratio=1), 4, 4)


def validate_breaks(br, x1, x2, expected):
    assert allclose(br((x1, x2)), expected)
    assert allclose(br((x2, x1)), expected)


def test_interval_breaks():
    breaks = interval_breaks()
    validate_breaks(breaks, 1, 4, [1, 2, 3, 4])
    validate_breaks(breaks, -2, -5, [-5, -4, -3, -2])
    validate_breaks(breaks, -2, 2, [-2, -1, 0, 1, 2])
    validate_breaks(breaks, 1.34, 4.89, [2, 3, 4])
    validate_breaks(breaks, -2.13, -5.408, [-5, -4, -3])
    validate_breaks(breaks, -2.62, 2.13, [-2, -1, 0, 1, 2])

    breaks = interval_breaks(0.2)
    validate_breaks(breaks, 0, 1, [0, 0.2, 0.4, 0.6, 0.8, 1])
    validate_breaks(breaks, 0.2, 1, [0.2, 0.4, 0.6, 0.8, 1])
    validate_breaks(breaks, 0.1, 0.99, [0.2, 0.4, 0.6, 0.8])

    breaks = interval_breaks(100)
    validate_breaks(breaks, 150, 250, [200])
    validate_breaks(breaks, 0, 1, [])


def validate_ticks(ggp, axis, expected):
    # Check that a rendered ggplot has the expected axis ticks.
    fig = ggp.draw()
    ax = fig.get_axes()[0]
    get_ticklabs = getattr(ax, "get_{}ticklabels".format(axis))
    tick_labs = [t.get_text() for t in get_ticklabs()]
    assert tick_labs == expected


def test_comma_fmt(largenum_df):
    base_ggp = gg.ggplot(largenum_df, gg.aes(x="x", y="y")) + gg.geom_point()
    other_labs = lambda vals: ["{:.1f}".format(x) for x in vals]

    ggp = base_ggp + x_comma_fmt()
    validate_ticks(ggp, "x", ["0", "1,000", "2,000", "3,000", "4,000"])
    ggp = base_ggp + x_comma_fmt(breaks=[1000, 3000])
    validate_ticks(ggp, "x", ["1,000", "3,000"])
    ggp = base_ggp + x_comma_fmt(labels=other_labs)
    validate_ticks(ggp, "x", ["0.0", "1000.0", "2000.0", "3000.0", "4000.0"])

    ggp = base_ggp + y_comma_fmt()
    validate_ticks(ggp, "y", ["-2,000", "0", "2,000", "4,000"])
    ggp = base_ggp + y_comma_fmt(breaks=[-1000, 1000, 3000])
    validate_ticks(ggp, "y", ["-1,000", "1,000", "3,000"])
    ggp = base_ggp + y_comma_fmt(labels=other_labs)
    validate_ticks(ggp, "y", ["-2000.0", "0.0", "2000.0", "4000.0"])
