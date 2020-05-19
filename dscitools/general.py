"""
General utilities.
"""

from __future__ import division
import datetime


def now():
    """Return a human-readable string giving current date and time."""
    return datetime.datetime.now().ctime()


def today():
    """Return today's date as an ISO-formatted date string."""
    return datetime.date.today().isoformat()


def fmt_count(
    n,
    description=None,
    n_overall=None,
    overall_description=None,
    show_n_overall=True,
    print_result=True,
):
    """Pretty-print a count together with some contextual information.

    This is generally useful for counts computed as part of an analysis or as a
    diagnostic. Although designed for integer counts, this can be used with any
    numeric value representing an amount.

    If `show_n_overall` is `True`, the result is a string of the form

        `"<description>: <n (pretty-printed)>" +
            " out of <n_overall> <description_overall>" +
            " (<percentage of n out of n_overall>)"`

    Otherwise, it produces a string of the form

        `"<description>: <n (pretty-printed)>" +
            " (<percentage of n out of n_overall> of <description_overall>)"`

    Parameters
    ----------
    n : numeric
        The number to be formatted
    description : str, optional
        Description of what the number `n` represents
    n_overall : numeric, optional
        Overall total, out of which `n` represents the amount in a subset
    description_overall : str, optional
        Description of what the overall total `n_overall` represents. This is
        useful if `n` is some specific subset out of a more general overall
        group. Ignored if `n_overall` is not given.
    show_n_overall : bool, optional
        Should the overall total be included in the formatted string? If
        `False` but `n_overall` is specified, the result will include the
        percentage value of `n` out of `n_overall`, but not `n_overall` itself.
        Ignored if `n_overall` is not given.
    print_result : bool, optional
        Should the formatted string be printed? If `False`, the string is
        returned rather than printed.

    Returns
    -------
    str or None
        If `print_result` is `False`, returns the formatted string. Otherwise,
        the string is printed but not returned.
    """
    # No rounding is currently done if n is not an integer.
    n_fmt = "{:,}"
    descr_fmt = "{}: "
    n_overall_fmt = "out of {:,}"
    # If overall_description is given and show_n_overall is False,
    # put the description in the parens together with the percentage.
    # Otherwise, it will be added separately, and 'descr' will be
    # an empty string.
    pct_fmt = " ({pct:.2%}{descr})"
    overall_descr_fmt = " of {}"

    components = []
    if description:
        components.append(descr_fmt.format(description))
    components.append(n_fmt.format(n))
    if n_overall:
        # The percentage will get shown regardless of whether show_n_overall
        # is True or False.
        # Note: if n_overall is 0, the percentage calculation is not attempted.
        pct_of_overall = n / n_overall
        if show_n_overall:
            components.append(n_overall_fmt.format(n_overall))
            if overall_description:
                # If the overall description is given, it follows naturally
                # right after the overall number without special formatting.
                components.append(overall_description)
            components.append(pct_fmt.format(pct=pct_of_overall, descr=""))
        else:
            # If an overall description is given, format and insert it
            # into the parens next to the percentage.
            if overall_description:
                overall_descr_str = overall_descr_fmt.format(
                    overall_description
                )
            else:
                overall_descr_str = ""
            components.append(
                pct_fmt.format(pct=pct_of_overall, descr=overall_descr_str)
            )
    formatted_str = " ".join(components)
    if print_result:
        print(formatted_str)
    else:
        return formatted_str
