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

## Project Output

The a **web platform** that allows users to:

- receive personalized book recommendations based on reading history
- find popular books books in the Seattle Public Library
- discover book clubs and reading events in the Seattle area tailored to preferences
- organize and manage book club activities

The project will also produce cleaned and transformed datasets, indexed for efficient lookup 
and easy use in the recommendation algorithm.

---

## Data Sources

This project uses the following data sources:

1. **Amazon Reviews Dataset** (2023 by McAuley Lab)
2. **Seattle Public Library API**
   - Checkouts by Title
4. **Seattle-area Book Club Event Data**

   *Scraped from Google events with SeprAPI

These datasets are processed using scripts in the `data/scripts` directory to generate cleaned datasets used by the application.

---

### Usage

1. **Amazon Reviews Dataset** (2023 by McAuley Lab)

Amazon book metadata provides information displayed in the user interface, including title, author, description, genres, average ratings, and cover images. Metadata features such as average rating, genre, and popularity are also used in the recommender to help rank recommended books. 

Amazon review data is used to construct sample user reading histories by converting reviews into a user-book interaction matrices. These interactions  represent users’ book preferences and are split in to training and test sets. The training (input) data is used to compute a cosine similarity matrix between books, a feature in our reccomendation algorithm.

2. **Checkouts by Title**

The SPL checkouts dataset provides a measure of local popularity that can be used to recomend books from SPL catalogue if a user toggles on SPL book reccomendations.

3. **Seattle-area Book Club Event Data**

The book events dataset provides information displayed in the user interface including name of bookclub, link to bookclub, thumbnail image, description, location, 
time, and books and genres the club is reading (most book clubs will not have all info). Bookclub features such as location, time, and genre are also used in bookclub 
recomender to rank recommended bookclubs

---

### Setup to Run Data Processing Scripts

#### Amazon Reviews Dataset

Dataset download page: https://amazon-reviews-2023.github.io/

Directions:
Under "Grouped by Category" download books `review` and `meta`. 

From Gzip file extract the following files and place in `data/raw`
- `Books.jsonl`
- `meta_Books.jsonl`

#### SPL API

Seattle Open Data portal: https://data.seattle.gov/

Directions:
1. create an account for Tyler's Data & Insights
2. go to "Developer Settings"
3. create new API Key and new App Token
4. make .env in spl_data containing SPL_TOKEN = "your_app_token"

#### SerpAPI
SerpAPI Dashboard: https://serpapi.com/dashboard

Directions:
1. create SerpAPI account 
2. in account you should see "Your Private API Key"
3. make .env in scripts containing api_key = "your_private_api_key"

---

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

## Data sources

This project integrates multiple data sources:

- **Amazon Books Dataset**: book metadata and recommendation signals (ratings, categories, images, etc.)
- **Seattle Public Library (SPL) data**: local trending/checkout popularity signals
- **Event data**: book clubs/events discovery (title, description, location, date/time, link)

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
The exact environment variables and AWS resources depend on your deployment configuration, but the expected deployed behavior is:

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
