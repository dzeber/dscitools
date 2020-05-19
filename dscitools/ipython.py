"""
Utilities for displaying information with IPython in the Jupyter notebook.
"""

from __future__ import print_function
from .general import now

# Keep the use of IPython display() optional.
# If not available, fall back to the standard print().
try:
    from IPython.display import display
except ImportError:
    display = print


def print_md(markdown_text):
    """Display text rendered as Markdown, if available on the client.

    If not, this will fall back on printing the raw text.

    Parameters
    ----------
    markdown_text : str
        The Markdown text to be displayed.
    """
    # Markdown can also be displayed using IPython.display.Markdown,
    # but this doesn't fall back elegantly when rich display is not available
    # (eg. in the console).
    md_message = MarkdownMessage(markdown_text)
    display(md_message)


def print_status(message):
    """Display a status message together with the current date and time.

    This is useful for printing status updates during the execution of a script
    or notebook. If Markdown display is available, it will appear bolded.

    Parameters
    ----------
    message : str
        The status text to be displayed.
    """
    status_message = StatusMessage(message)
    display(status_message)


def print_assertion(description, result):
    """Display a message informing whether an assumption holds.

    This can be used like an assertion statement, although failures don't halt
    execution. It is primarily intended for reporting on the properties of
    objects created while running a script, which may vary if they depend on
    external data. If available on the client, the message will be rendered as
    Markdown.

    Parameters
    ----------
    description : str
        A string describing the assumption.
    result : bool
        The computed outcome of checking the assumption.
    """
    assertion = AssertMessage(description, result)
    display(assertion)


class MarkdownMessage(object):
    """A message that renders as Markdown in IPython rich output.

    If rich Markdown display is available on the client (via
    `_repr_markdown_`), the text will be rendered as Markdown when
    `display()`-ed. Otherwise, it falls back on printing the raw text.

    Parameters
    ----------
    message : str
        The Markdown text to be displayed.
    """

    def __init__(self, message):
        self._message = message

    @classmethod
    def _encode_markdown(cls, message):
        """Make transformations to the message prior to rendering as Markdown.

        The main thing is to convert spaces to non-breaking spaces, so that
        plaintext spacing is preserved.
        """
        md_message = message.replace(" ", "&nbsp;")
        return md_message

    def _repr_markdown_(self):
        """Return the Markdown representation of the text.

        If supported by the client, it will get rendered as HTML.
        """
        return self._encode_markdown(self._message)

    def __repr__(self):
        """The fallback version, when Markdown display is not available.

        This will just print the raw Markdown text, without rendering.
        """
        return self._message


class StatusMessage(MarkdownMessage):
    """A status message that includes the current time and date.

    The timestamp is computed every time the message is displayed. The message
    will be bolded if Markdown is available on the client.

    Parameters
    ----------
    message : str
        The status message to be displayed.
    """

    MESSAGE_TEMPLATE = "{msg}:  {time}"
    MARKDOWN_TEMPLATE = "__{}__"

    def _create_timestamped_message(self):
        """Format the status message with the current timestamp."""
        return self.MESSAGE_TEMPLATE.format(msg=self._message, time=now())

    def __repr__(self):
        """Print the raw text of the status message."""
        return self._create_timestamped_message()

    def _repr_markdown_(self):
        """Render the status message as Markdown, if available on the client."""
        ts_message = self._create_timestamped_message()
        md_message = self.MARKDOWN_TEMPLATE.format(ts_message)
        return self._encode_markdown(md_message)


class AssertMessage(MarkdownMessage):
    """An assertion message for use in data processing.

    A message describing the expected behaviour is displayed, together with a
    computed result. This is only intended to be informational, and does not
    halt execution. The message will be rendered as Markdown if available on
    the client.

    Parameters
    ----------
    message : str
        The assertion message to be displayed.
    result : bool
        The outcome of the assertion test.
    """

    MESSAGE_TEMPLATE = "__Assert__  {msg}:  `{result}`"

    def __init__(self, message, result):
        self._message = self.MESSAGE_TEMPLATE.format(
            msg=message, result=result
        )
