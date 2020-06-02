from dscitools.plotting import figsize


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
