# Bookish: A Book Club Manager

This repository contains the code and data processing scripts used to build **Bookish**, a platform for discovering books, organizing book clubs, and connecting with reading communities in the Seattle area.

---

## Project Group Members
- Ha Tien Nguyen  
- Sarah Mathison  
- Maanya Cola Bharath  
- Elsie Wang  

---

## Project Type

Bookish is a data-driven web application built using **Streamlit** for the frontend interface.

# Questions of Interest

This project explores several questions related to book discovery and reading communities:

- How can user reviews data and book meta data be combined to recommend books that readers are likely to enjoy?
- How can public library data help users locate books available in their local area?
- How can users discover book clubs and reading communities that match their interests?

---

## Project Output

The final output of the project will be a **web platform** that allows users to:

- receive personalized book recommendations based on genre preferences and reading history
- find popular books in the Seattle Public Library
- discover book clubs and reading events in the Seattle area tailored to preferences
- organize and manage book club activities

The project will also produce cleaned and transformed datasets, indexed for efficient lookup 
and easy use in the recommendation algorithm.

---

## Data Sources

This project uses the following data sources:

1. **Amazon Reviews Dataset** (2023 by McAuley Lab)
2. **Seattle Public Library API**
   - Library Collection Inventory
   - Checkouts by Title
4. **Seattle-area Book Club Event Data**

   *Scraped from Google events with SeprAPI

These datasets are processed using scripts in the `data/scripts` directory to generate cleaned datasets used by the application.

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

## Run Streamlit App

```bash
conda activate bookish
pip install -r requirements.txt
streamlit run streamlit_app.py
```
* Note if running locally you will have to obtain model artifacts by running the data scripts as specified in 'README_recommender_pipeline.txt'


## Repository Structure

`Book-Club-Manager`

│

├── `backend/`                Backend server code

│   ├── `services/`          backend service functions

│   ├── `recommender/`       recommender scripts

│ 

├── `frontend/`               Frontend UI

├── `data/`

│   ├── `raw/`                Raw input datasets (Large files not stored in repo)

│   ├── `processed/`          Data processing scripts write the output files to this folder. Currently includes small                                      snippets of cleaned datasets. 

│   └── `scripts/`            Data cleaning and preprocessing scripts

