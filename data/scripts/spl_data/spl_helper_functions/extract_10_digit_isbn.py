"""
Helper function to extract a valid 10-digit ISBN from a comma-separated ISBN field. "
This function is used in the SPL checkout data processing to create an index by ISBN. 
It handles both 10 and 13 digit ISBN formats and converts ISBN-13 to ISBN-10 when possible.
"""


import isbnlib


def extract_isbn10(isbn_field):
    """
    Extracts a valid ISBN-10 from a comma-separated ISBN field. If an ISBN-13 is
    encountered, it is converted to ISBN-10 when possible.

    Args:
        isbn_field (str or None): A string containing one or more ISBN values
                                  separated by commas. Values may include ISBN-10 or ISBN-13
                                  and may contain hyphens, spaces, or other formatting.

    Returns:
        str or None: A valid 10-digit ISBN if one can be extracted or converted.
        Returns None if no valid ISBN-10 or convertible ISBN-13 is found.
    """

    if isbn_field is None:
        return None
    for candidate in str(isbn_field).split(","):
        cleaned = isbnlib.clean(candidate)
        if not cleaned:
            continue
        if isbnlib.is_isbn10(cleaned):
            return cleaned
        if isbnlib.is_isbn13(cleaned):
            try:
                return isbnlib.to_isbn10(cleaned)
            except ValueError:
                pass

    return None
