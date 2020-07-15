"""
Utilities for plotting with GGPlot.
"""

from __future__ import division

from numpy import amin, amax, floor, ceil, arange
import plotnine as gg
import mizani as scales

# Default width in inches
DEFAULT_PLOT_WIDTH = 8
# Default ratio height:width
DEFAULT_RATIO = 0.617
# Default ratio width:height
DEFAULT_RATIO_INVERSE = 1.62


def _compute_figure_size(width=None, height=None, ratio=None):
    """Logic to compute figure dimensions with sensible defaults."""
    if not height:
        # Use ratio to determine the height.
        width = width or DEFAULT_PLOT_WIDTH
        ratio = ratio or DEFAULT_RATIO
        height = width * ratio
    elif height and not width:
        # Use ratio to determine the width.
        ratio = ratio or DEFAULT_RATIO_INVERSE
        width = height * ratio

    return (width, height)


def figsize(width=None, height=None, ratio=None):
    """Set the figure size.

    If both `width` and `height` are specified explicitly, these dimensions are
    used. If only one of `width` and `height` are given, the other is computed
    as the result of multiplying by `ratio` (or a default if not supplied).
    Otherwise, if neither are given, width is set to its default value and
    height is computed using the ratio.

    Returns a ggplot theme with `figure_size` set that can be added to a plot.
    """
    return gg.theme(
        figure_size=_compute_figure_size(
            width=width, height=height, ratio=ratio
        )
    )


def interval_breaks(interval=1):
    """Breaks for a continuous scale at fixed intervals.

    Returns a function that can be passed to the `breaks` arg
    of a continuous scale, generating regularly-spaced breaks
    at the specified `interval` size.
    """

    def generate_breaks(limits):
        """ Compute breaks at set intervals between the given limits. """
        start = ceil(amin(limits) / interval) * interval
        end = floor(amax(limits) / interval) * interval
        return arange(start, end + interval * 0.5, step=interval)

    return generate_breaks


def _comma_fmt(axis, **kwargs):
    """Continuous axis with comma formatting.

    kwargs: additional params passed to the ggplot scale function. These take
    precedence.
    """
    args = {"labels": scales.formatters.comma_format()}
    args.update(kwargs)
    axis_func = getattr(gg, "scale_{}_continuous".format(axis))
    return axis_func(**args)


def x_comma_fmt(**kwargs):
    """Continuous `x` axis with comma formatting for large numbers.

    kwargs: additional params passed to the ggplot scale function. These take
    precedence.
    """
    return _comma_fmt("x", **kwargs)


def y_comma_fmt(**kwargs):
    """Continuous `y` axis with comma formatting for large numbers.

    kwargs: additional params passed to the ggplot scale function. These take
    precedence.
    """
    return _comma_fmt("y", **kwargs)
