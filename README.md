[![CI](https://github.com/Ha-Tien-121/Book-Club-Manager/actions/workflows/build_test.yml/badge.svg)](https://github.com/Ha-Tien-121/Book-Club-Manager/actions/workflows/build_test.yml)
[![Coverage Status](https://coveralls.io/repos/github/Ha-Tien-121/Book-Club-Manager/badge.svg?branch=main)](https://coveralls.io/github/Ha-Tien-121/Book-Club-Manager?branch=main)

# Bookish: A Book Club Manager

Repository: `https://github.com/Ha-Tien-121/Book-Club-Manager`

Bookish is a Streamlit app that centralizes book club experiences in one place:

- Discover book clubs and events
- Manage a personal library (saved / in-progress / finished)
- Participate in book discussions (forum)
- Receive personalized book recommendations

---

## Contributors

- Ha Tien Nguyen
- Sarah Mathison
- Maanya Cola Bharath
- Elsie Wang

---

## What this project delivers

Bookish is a web platform that allows users to:

- receive personalized book recommendations based on reading history
- find popular books in the Seattle Public Library system
- discover Seattle-area book clubs and reading events
- organize and manage personal library and book-club activity

The project also includes data pipelines that clean, transform, and load artifacts
used by the app and recommendation workflows.

---

## Data sources

This project integrates multiple data sources:

1. **Amazon Books/Reviews Dataset** (McAuley Lab, 2023)
2. **Seattle Public Library (SPL) data** (checkouts and catalog-derived popularity)
3. **Seattle-area book events** (collected via SerpAPI/Google Events)

How each source is used:

- **Amazon Books/Reviews** -> Primary catalog + recommendation backbone. We use fields such as `parent_asin`, title, author, categories/genres, ratings, image URL, and description to build `books.db`, Parquet shards, and recommender artifacts consumed by the feed/library views.  
  Limitation: Coverage and recommendation quality depend on metadata completeness and review density.
- **SPL data** -> Seattle-local demand signal. Checkout counts are joined against books to produce `spl_top50_checkouts_in_books.json`, which powers trending/local discovery and helps rank recommendations toward local relevance.  
  Limitation: Reflects SPL circulation behavior and may not represent broader reader preferences.
- **Book events (SerpAPI/Google Events)** -> Event discovery and personalization source. Event title, venue, date/time, description, and links are normalized and tagged for explore-events pages and event recommendation candidates.  
  Limitation: Freshness/completeness depends on third-party event listings and scraping/API availability.

## Project structure (what’s where)

Top-level:

- `README.md`: project overview + setup/run/test instructions
- `LICENSE`: MIT License
- `pyproject.toml`: packaging metadata (installable project)
- `docs/`: project documentation (functional spec, component/design spec, technology reviews, final project presentation)
- `examples/`: user walkthroughs based on the functional specification
- `tests/`: automated tests (`pytest`)

Inner application code:

- `Book-Club-Manager/`:
  - `backend/`: storage + services + recommenders
  - `frontend/`: Streamlit UI pages and components
  - `data/`: scripts + processed datasets used by the app
  - `streamlit_app.py`: actual Streamlit app module

Entrypoint shim:

- `streamlit_app.py` (repo root): a small shim that loads `Book-Club-Manager/streamlit_app.py`
  - This is helpful for CI/test tooling and deployments that expect an app entrypoint at repo root.

## Documentation

- **Functional specification**: `docs/Functional_spec.md`
- **Component / design specification**: `docs/Component_spec.md`
- **Technology reviews**: `docs/technology_review/`
- **Final presentation**: `docs/final_presentation/`

## Examples (user walkthroughs)

The rubric requires step-by-step user interaction walkthroughs.
See:

- `examples/walkthrough_rayleigh_casual_reader.md`
- `examples/walkthrough_ben_professional_learner.md`

## Deployment

Deployed Streamlit app: `http://100.23.182.233/`

Automated deployment to EC2 is configured in `.github/workflows/deploy_ec2.yml` and runs after CI (`.github/workflows/build_test.yml`) succeeds on `main`.

## Setup (reproducible)

### Prerequisites

- Python 3.11 recommended
- Git
- (Optional) Conda if you prefer conda environments

### Option A: Conda (recommended if you have `bookish_env.yml`)

```bash
conda env create -f bookish_env.yml
conda activate bookish-env
pip install -r requirements.txt
pip install -e .
```

### Option B: Python venv

```bash
python -m venv .venv

# Windows PowerShell:
. .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
pip install -e .
```

## Credentials and raw data acquisition

Required before running data pipelines.

### API keys / tokens

- **SPL token (`SPL_TOKEN`)**
  - Get it from [Seattle Open Data](https://data.seattle.gov/) (Tyler Data & Insights -> Developer Settings -> App Token).
  - Export as `SPL_TOKEN` (shell or `.env`).

- **SerpAPI key (`api_key`)**
  - Get it from [SerpAPI Dashboard](https://serpapi.com/dashboard).
  - Export as `api_key` (shell or `.env`).

### Raw data files

From [Amazon Reviews 2023](https://amazon-reviews-2023.github.io/), download books
`review` and `meta`, then place:

- `Book-Club-Manager/data/raw/meta_Books.jsonl`
- `Book-Club-Manager/data/raw/Books.jsonl`

If not fetching events live, also place:

- `Book-Club-Manager/data/raw/book_events_raw.json`

## Data pipeline runbooks

This project has two runtime modes (`local` and `aws`) but both depend on the same
data preparation steps first.

### Before either mode: prepare data artifacts

From the repository root:

```bash
cd Book-Club-Manager
```

Run the processing scripts:

```bash
# Build cleaned books.db + book_id_to_idx.json
python -m data.scripts.amazon_books_data.books_meta_data

# Build sharded parquet files from books.db
python data/scripts/shard_books_by_prefix.py --source data/processed/books.db --out-dir data/shards/parent_asin

# Build recommender artifacts
python data/scripts/build_recommender_artifacts.py

# Event pipeline (fetch raw, then clean)
python data/scripts/events/get_book_events.py
python data/scripts/events/clean_book_events.py
```

### Local mode runbook

After data preparation:

```bash
cd ..
streamlit run streamlit_app.py
```

Local mode behavior:
- Uses local processed artifacts/files where available.
- Does not require DynamoDB/S3 for normal local development.

### AWS mode runbook (includes loaders)

AWS mode requires all preparation steps above **plus** uploading/loading artifacts
to S3/DynamoDB.

Before running loaders, make sure:

- AWS credentials are available (EC2 IAM role or `aws configure` profile).
- `DATA_BUCKET` is set.
- DynamoDB table env vars are set if you are not using defaults.
- API keys are set for data generation steps that need them (`SPL_TOKEN`, `api_key`).

From `Book-Club-Manager/`:

```bash
# DynamoDB loaders
python -m data.scripts.loaders.load_books_to_dynamodb --all
python data/scripts/loaders/load_events_to_dynamodb.py

# Build SPL top-50 JSON (depends on SPL token + books table)
python -m data.scripts.spl_data.spl_checkout_data

# S3 loaders
python data/scripts/loaders/load_book_shards_to_s3.py
python -m data.scripts.loaders.load_spl_top50_to_s3
python data/scripts/loaders/load_reviews_top50_to_s3.py
```

Set deployment environment variables so runtime uses cloud storage:
- `APP_ENV=aws`
- `AWS_REGION=us-west-2`
- `DATA_BUCKET=<your bucket>`
- table env vars when non-default (`BOOKS_TABLE`, `EVENTS_TABLE`, etc.)

## Running the app locally (LocalStorage mode)

From the repo root:

```bash
streamlit run streamlit_app.py
```

What “local mode” means:

- The app loads bootstrap/book/event/forum data from local processed files where available.
- Some features (especially cloud persistence) are not used; the app uses local storage behavior.

## Running the app on AWS (CloudStorage mode)

The repository supports an AWS-backed runtime mode used for deployment (DynamoDB/S3).
See `Data pipeline runbooks` above for required data preparation and loader steps.
Expected runtime behavior:

- `APP_ENV=aws`
- `config.IS_AWS == True`
- `get_storage()` returns `CloudStorage`

CI/CD:

- **CI workflow**: `.github/workflows/build_test.yml`
- **EC2 deploy workflow**: `.github/workflows/deploy_ec2.yml`
  - Deploy triggers after CI succeeds on `main` (or can be run manually via workflow dispatch).

Health checks (common in deployed Streamlit):

- Streamlit internal health endpoint: `http://127.0.0.1:8501/_stcore/health`

## Tests

Run the full test suite:

```bash
pytest -q
```

Notes:

- Some UI/integration-style tests may be skipped if fixture apps or external dependencies are not present.
- The goal is deterministic, CI-friendly unit/integration behavior without requiring AWS credentials locally.

## Coverage (how we measure it + current values)

### What the numbers mean

Coverage depends on the `--source` target:

- **Total coverage** measures the entire inner project (`Book-Club-Manager/`) including backend + frontend + data scripts.
- **Backend coverage** measures only `Book-Club-Manager/backend/`.

This distinction is important because data pipeline scripts and UI pages can significantly affect the overall percentage.

### Current coverage targets (project state at time of writing)

- **Total coverage: 77%**
  - Whole-project coverage for `Book-Club-Manager/` (backend + frontend + data scripts)
- **Backend coverage: 85%**
  - Backend-only coverage for `Book-Club-Manager/backend/`

### Commands: total coverage

```bash
coverage erase
coverage run --source=Book-Club-Manager -m pytest -q
coverage report -m
```

### Commands: backend-only coverage

```bash
coverage erase
coverage run --source=Book-Club-Manager/backend -m pytest -q
coverage report -m
```

Tip: to show backend-only lines after a total run (quick breakdown):

```bash
# mac/linux:
coverage report -m | grep "Book-Club-Manager/backend"

# Windows PowerShell:
coverage report -m | Select-String "Book-Club-Manager\\backend\\|TOTAL"
```

## Linting / code quality

CI includes linting via GitHub Actions (`.github/workflows/build_test.yml`).

## License

MIT License. See `LICENSE`.
