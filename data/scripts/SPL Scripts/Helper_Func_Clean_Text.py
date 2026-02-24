def clean_text(series):
    """
    Cleans a pandas Series of text by:
    (1) removing quotes
    (2) removing slash and following text (SPL titles often have format "Title / extra info")
    (3) lowercasing
    (4) trimming whitespace on edges
    (5) normalizing multiple spaces to a single space
    """
    return (
        series
        .astype(str)
        .str.replace('"', '', regex=False)        # remove quotes
        .str.replace(r'/.*', '', regex=True)     # remove slash and after
        .str.lower()                             # lowercase
        .str.strip()                             # trim edges
        .str.replace(r'\s+', ' ', regex=True)    # normalize whitespace
    )