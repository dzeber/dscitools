"""
Utilities for plotting with GGPlot.
"""

from plotnine import theme

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
    return theme(
        figure_size=_compute_figure_size(
            width=width, height=height, ratio=ratio
        )
    )
