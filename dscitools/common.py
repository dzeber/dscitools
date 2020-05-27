import six


def is_str(x):
    return isinstance(x, six.string_types)


def to_str(x):
    if not is_str(x):
        x = unicode(x) if six.PY2 else str(x)  # noqa: F821
    return x
