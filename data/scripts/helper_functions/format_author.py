"Helper functions for formatting author name in the SPL and Amazon datasets."


import pandas as pd

def format_author(authors):
    """
    Format author names to make more readable.

    Formats author names by:
    (2) keeping only the first two comma-separated elements
    (4) stripping whitespace on edges

    Args:
        author (str, pd.Series): author name string "last, first, ..."

    Returns:
        str or pandas.Series: formatted author name (last, first) in the same type as input
    """

    def _format_single_author(author):
        author = str(author)
        parts = [p.strip() for p in author.split(",")]
        if len(parts) >= 2:
            return f"{parts[0]}, {parts[1]}"
        return parts[0]
    if not isinstance(authors, (pd.Series, str)):
        return None
    if isinstance(authors, pd.Series):
        return authors.apply(_format_single_author)
    return _format_single_author(authors)
