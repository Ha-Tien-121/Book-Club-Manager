import csv
import os
import pandas as pd

RAW_INPUT_PATH = os.getenv(
    "BOOKCLUBS_RAW_PATH", "data/raw/bookclubs_seattle_raw.csv"
)
CLEAN_OUTPUT_PATH = os.getenv(
    "BOOKCLUBS_CLEAN_PATH", "data/processed/bookclubs_seattle_clean.csv"
)


def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal cleaning:
    - strip whitespace
    - drop rows missing title or link
    - drop duplicate links
    - reorder columns
    """
    if df.empty:
        return df

    df = df.copy()
    str_cols = [
        "query",
        "title",
        "link",
        "description",
        "when",
        "start_date",
        "end_date",
        "address",
        "venue",
        "location",
        "thumbnail",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # keep only rows with a title and link
    df = df[df["title"].astype(bool) & df["link"].astype(bool)]
    df = df.drop_duplicates(subset="link")

    # Reorder/select known columns; keep unknowns at the end
    ordered_cols = [c for c in str_cols if c in df.columns]
    remaining_cols = [c for c in df.columns if c not in ordered_cols]
    df = df[ordered_cols + remaining_cols]
    return df


def main():
    if not os.path.exists(RAW_INPUT_PATH):
        raise SystemExit(f"Raw input not found: {RAW_INPUT_PATH}")

    raw_df = pd.read_csv(RAW_INPUT_PATH)
    clean_df = clean_events(raw_df)

    os.makedirs(os.path.dirname(CLEAN_OUTPUT_PATH), exist_ok=True)
    clean_df.to_csv(
        CLEAN_OUTPUT_PATH,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_ALL,
    )
    print(f"Cleaned {len(clean_df)} events -> {CLEAN_OUTPUT_PATH}")


if __name__ == "__main__":
    main()

