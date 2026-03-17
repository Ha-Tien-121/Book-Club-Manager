"""
Fetch, clean, and aggregate Seattle Public Library checkout data by ISBN only,
then join it with book data from DynamoDB, and output top 50 most popular books in Seattle.

Steps:
1. Retrieve checkouts from the last year (to the same month).
2. Filter to MaterialType in {BOOK, EBOOK, EAUDIOBOOK}.
3. Clean title and creator columns; convert Checkouts to numeric.
4. Extract a 10-digit ISBN from the ISBN column (if present).
5. Aggregate total Checkouts by ISBN.
6. Sort ISBNs by Checkouts (descending) and find up to 50 that exist as
   `parent_asin` in the DynamoDB `books` table.
7. Fetch the full DynamoDB items for those 50 books and attach the SPL
   checkout count as a `checkouts` field.

Outputs:
    - spl_top50_checkouts_in_books.json  (list of full book items + `checkouts`)

Time: ~7 minutes to run
"""

import json
import os

from datetime import datetime
from decimal import Decimal
import pandas as pd
from sodapy import Socrata
import boto3
from boto3.dynamodb.types import TypeDeserializer
from dotenv import load_dotenv

from data.scripts.helper_functions.format_title import format_title
from data.scripts.helper_functions.format_author import format_author

from data.scripts.spl_data.spl_helper_functions.extract_10_digit_isbn import extract_isbn10


load_dotenv()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
OUTPUT_TOP50_CHECKOUTS_IN_BOOKS = os.path.join(
    BASE_DIR,
    'data',
    'processed',
    'spl_top50_checkouts_in_books.json',
)

APP_TOKEN = os.getenv("SPL_TOKEN")
BOOKS_TABLE = os.getenv("BOOKS_TABLE", "books")

# Diversity / quality constraints for top-50 SPL checkouts
# Cap Children's Books specifically to keep overall variety; other genres are uncapped.
MAX_CHILDRENS_BOOKS = int(os.getenv("SPL_TOP50_MAX_CHILDRENS", "10"))


def _get_top_existing_isbns_in_dynamo(
    ordered_isbns: list[str],
    max_matches: int = 50,
    table_name: str = BOOKS_TABLE,
) -> list[str]:
    """
    Given ISBNs ordered by descending checkouts, return up to `max_matches` ISBNs
    that exist as parent_asin in the DynamoDB books table.

    Stops early once `max_matches` matches have been found to minimize calls.
    """
    if not ordered_isbns or max_matches <= 0:
        return []

    dynamodb = boto3.client("dynamodb")
    matches: list[str] = []

    # Clean and de-duplicate while preserving order.
    seen = set()
    cleaned: list[str] = []
    for isbn in ordered_isbns:
        if not isbn:
            continue
        s = str(isbn)
        if s in seen:
            continue
        seen.add(s)
        cleaned.append(s)

    batch_size = 100
    for i in range(0, len(cleaned), batch_size):
        if len(matches) >= max_matches:
            break

        chunk = cleaned[i:i + batch_size]
        request_items = {
            table_name: {
                "Keys": [{"parent_asin": {"S": isbn}} for isbn in chunk],
                "ProjectionExpression": "parent_asin",
            }
        }
        resp = dynamodb.batch_get_item(RequestItems=request_items)
        found_in_this_batch = {
            item.get("parent_asin", {}).get("S")
            for item in resp.get("Responses", {}).get(table_name, [])
            if item.get("parent_asin", {}).get("S")
        }

        for isbn in chunk:
            if isbn in found_in_this_batch and isbn not in matches:
                matches.append(isbn)
                if len(matches) >= max_matches:
                    break

        # Retry unprocessed keys, still respecting early-stop when possible.
        unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        while unprocessed and len(matches) < max_matches:
            retry_req = {
                table_name: {
                    "Keys": unprocessed,
                    "ProjectionExpression": "parent_asin",
                }
            }
            retry_resp = dynamodb.batch_get_item(RequestItems=retry_req)
            retry_found = {
                item.get("parent_asin", {}).get("S")
                for item in retry_resp.get("Responses", {}).get(table_name, [])
                if item.get("parent_asin", {}).get("S")
            }
            for key in unprocessed:
                p = key.get("parent_asin", {}).get("S")
                if p in retry_found and p not in matches:
                    matches.append(p)
                    if len(matches) >= max_matches:
                        break
            unprocessed = retry_resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])

    return matches


def _batch_get_books(parent_asins: list[str], table_name: str = BOOKS_TABLE) -> dict[str, dict]:
    """
    Fetch full DynamoDB book items for the given parent_asins.
    Returns a dict mapping parent_asin -> item.
    """
    if not parent_asins:
        return {}

    dynamodb = boto3.client("dynamodb")
    deserializer = TypeDeserializer()
    items_by_id: dict[str, dict] = {}

    # Clean and de-duplicate while preserving order.
    seen = set()
    cleaned: list[str] = []
    for asin in parent_asins:
        if not asin:
            continue
        s = str(asin)
        if s in seen:
            continue
        seen.add(s)
        cleaned.append(s)

    batch_size = 100
    for i in range(0, len(cleaned), batch_size):
        chunk = cleaned[i:i + batch_size]
        request_items = {
            table_name: {
                "Keys": [{"parent_asin": {"S": asin}} for asin in chunk],
            }
        }
        resp = dynamodb.batch_get_item(RequestItems=request_items)
        for raw in resp.get("Responses", {}).get(table_name, []):
            asin = raw.get("parent_asin", {}).get("S")
            if not asin:
                continue
            # Convert DynamoDB attribute structure to a simple dict using boto3's deserializer.
            items_by_id[asin] = {k: deserializer.deserialize(v) for k, v in raw.items()}

        unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        while unprocessed:
            retry_req = {table_name: {"Keys": unprocessed}}
            retry_resp = dynamodb.batch_get_item(RequestItems=retry_req)
            for raw in retry_resp.get("Responses", {}).get(table_name, []):
                asin = raw.get("parent_asin", {}).get("S")
                if not asin:
                    continue
                items_by_id[asin] = {k: deserializer.deserialize(v) for k, v in raw.items()}
            unprocessed = retry_resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])

    return items_by_id


def _to_jsonable(obj):
    """
    Recursively convert DynamoDB-deserialized objects into something JSON-serializable.
    - Decimal -> float or int
    - Lists / dicts -> walk structure
    """
    if isinstance(obj, Decimal):
        # Use int when it is integral, otherwise float.
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, list):
        return [_to_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    return obj

def main(  # pylint: disable=too-many-nested-blocks
    output_top50_in_books=OUTPUT_TOP50_CHECKOUTS_IN_BOOKS,
    client=None,
):
    """
    Fetches, cleans, and aggregates SPL book checkout data from the past year,
    summing checkouts by ISBN (if available) or by Title and Author, and writes
    two JSON indexes for lookup by ISBN and by normalized title|author.
    """
    if client is None:
        if not APP_TOKEN:
            raise ValueError("SPL_TOKEN environment variable is required.")
        client = Socrata("data.seattle.gov", APP_TOKEN, timeout=120)
    current_year = datetime.now().year
    current_month = datetime.now().month
    material_types = ("BOOK", "EBOOK", "EAUDIOBOOK")
    chunks = []
    offset = 0
    while True:
        where_clause = (
            f"((CheckoutYear = {current_year - 1} "
            f"AND CheckoutMonth >= {current_month}) "
            f"OR (CheckoutYear = {current_year})) "
            f"AND MaterialType IN {material_types}"
            )
        checkouts = client.get(
            "tmmm-ytt6",
            select="Title, Creator, Checkouts, ISBN",
            where=where_clause,
            limit=10000,
            offset=offset)
        if not isinstance(checkouts, list) or len(checkouts) == 0:
            break
        checkouts_df = pd.DataFrame(checkouts)
        checkouts_df["Title"] = format_title(checkouts_df["Title"])
        checkouts_df["Creator"] = format_author(checkouts_df["Creator"])
        checkouts_df["Checkouts"] = pd.to_numeric(checkouts_df["Checkouts"],
                                        errors='coerce').fillna(0).astype(int)
        checkouts_df["ISBN"] = checkouts_df["ISBN"].apply(extract_isbn10)
        chunks.append(checkouts_df)
        offset += 10000
    if not chunks:
        raise ValueError("No data retrieved from SPL API. Please check the API query and " \
        "parameters.") 
    checkouts_df_all = pd.concat(chunks, axis=0, ignore_index=True)
    checkouts_df_all.rename(columns={'Creator': 'Author'}, inplace=True)

    # Only keep rows with a valid ISBN; we no longer support aggregation by title/author only.
    checkouts_with_isbn = checkouts_df_all[pd.notna(checkouts_df_all["ISBN"])].copy()
    if checkouts_with_isbn.empty:
        raise ValueError("No rows with valid ISBNs found in SPL data.")

    grouped_isbn = (
        checkouts_with_isbn.groupby('ISBN', as_index=False)['Checkouts'].sum()
        .merge(checkouts_with_isbn.groupby('ISBN')['Title'].first(), on='ISBN', how='left')
        .merge(checkouts_with_isbn.groupby('ISBN')['Author'].first(), on='ISBN', how='left')
    )

    grouped_isbn = grouped_isbn.where(pd.notnull(grouped_isbn), None)
    checkouts_json = grouped_isbn.to_dict(orient='records')

    books_by_isbn = {}
    for row in checkouts_json:
        isbn = row.get("ISBN")
        if not isbn:
            # Should not happen, since we filtered to rows with ISBN above.
            continue
        books_by_isbn[isbn] = {
            "Title": row.get("Title"),
            "Author": row.get("Author"),
            "ISBN": isbn,
            "Checkouts": int(row["Checkouts"]),
        }

    # Compute top 50 checkouts where ISBN exists as parent_asin in DynamoDB books table,
    # then fetch full book items and attach the SPL checkout count.
    if books_by_isbn:
        isbn_df = pd.DataFrame.from_records(
            [
                {
                    "ISBN": k,
                    "Title": v.get("Title"),
                    "Author": v.get("Author"),
                    "Checkouts": v.get("Checkouts", 0),
                }
                for k, v in books_by_isbn.items()
            ]
        )
        if not isbn_df.empty:
            # Sort SPL ISBNs by Checkouts descending first.
            isbn_df_sorted = isbn_df.sort_values("Checkouts", ascending=False)
            ordered_isbns = isbn_df_sorted["ISBN"].tolist()

            # Ask Dynamo only for as many as we need, stopping after 50 matches.
            top_existing_isbns = _get_top_existing_isbns_in_dynamo(
                ordered_isbns,
                max_matches=50,
            )
            if top_existing_isbns:
                # Fetch full book items for those parent_asins.
                books_from_dynamo = _batch_get_books(top_existing_isbns)

                # Build a lookup from ISBN to SPL checkouts.
                checkout_lookup = {
                    row["ISBN"]: int(row["Checkouts"])
                    for _, row in isbn_df_sorted.iterrows()
                    if row["ISBN"] in top_existing_isbns
                }

                # Preserve order by SPL checkouts (already sorted) and enrich each Dynamo item.
                # Additionally:
                # - Exclude books with missing/empty images (so cards always have a cover).
                # - Enforce variety by capping Children's Books specifically.
                # - Cap specific overrepresented authors (e.g., Mo Willems) to keep variety.
                enriched_items = []
                childrens_count = 0
                per_author_counts: dict[str, int] = {}

                for isbn in ordered_isbns:
                    if isbn not in top_existing_isbns:
                        continue
                    book = books_from_dynamo.get(isbn)
                    if not book:
                        continue

                    # Require a non-empty images field.
                    images_val = book.get("images")
                    if not images_val or (isinstance(images_val, str) and not images_val.strip()):
                        continue

                    # Cap Mo Willems titles to at most 3 in the list.
                    author_name = str(book.get("author_name") or "").strip()
                    author_key = author_name.lower()
                    if author_key == "mo willems":
                        if per_author_counts.get(author_key, 0) >= 3:
                            continue

                    # Determine primary genre/category from DynamoDB item (first category).
                    cats = book.get("categories") or []
                    if isinstance(cats, str):
                        try:
                            cats = json.loads(cats) if cats.strip() else []
                        except (ValueError, TypeError):
                            cats = []
                    primary_genre = str(cats[0]).strip() if isinstance(cats, list) and cats else "Unknown"
                    # Specifically cap Children's Books to keep list from being dominated by that genre.
                    if primary_genre == "Children's Books":
                        if childrens_count >= MAX_CHILDRENS_BOOKS:
                            continue

                    # Attach SPL checkout count; do not override any existing fields.
                    book_with_checkouts = dict(book)
                    book_with_checkouts["checkouts"] = checkout_lookup.get(isbn, 0)
                    enriched_items.append(_to_jsonable(book_with_checkouts))
                    if primary_genre == "Children's Books":
                        childrens_count += 1
                    if author_key:
                        per_author_counts[author_key] = per_author_counts.get(author_key, 0) + 1

                    if len(enriched_items) >= 50:
                        break

                if enriched_items:
                    with open(output_top50_in_books, "w", encoding="utf-8") as f:
                        json.dump(enriched_items, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
