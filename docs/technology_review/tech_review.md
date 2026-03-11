# Technology Review
## Requests vs. Selenium vs. Playwright

### Background

We needed to collect book club event data (when, where, what, and links) for our app. The core question was how to obtain this data. One consideration was using a lightweight HTTP client like `requests` to call SerpAPI, which scrapes Google Events and returns structured JSON, or using a browser automation tool like `playwright` to scrape event platforms directly. Scraping platforms like Meetup and Eventbrite directly was an option, but those sites explicitly prohibit scraping and automated data extraction in their Terms of Service. This choice shaped our architecture—API-first vs. direct scraping—and involved trade-offs around reliability, ToS compliance, quality control, and maintenance. We debated between `requests` (for API calls) and `playwright` (for scraping JS-heavy sites) as the primary approaches for fetching book club listings.

### Description

The `requests` Python library is a lightweight HTTP client for REST/JSON APIs. It speaks HTTP directly and is well-suited for calling SerpAPI, which scrapes Google Events and returns structured JSON for search results (including book club events). The `playwright` library, by contrast, automates a headless browser to load and interact with web pages. It was the main alternative we considered for scraping event platforms directly, since it can render JavaScript, handle dynamic content, and simulate user actions like clicking and scrolling—capabilities that `requests` lacks.

### Comparison

Both approaches have clear strengths. `requests` speaks HTTP/JSON directly, stays fast and lightweight, and avoids browser overhead—well-suited for calling SerpAPI, which scrapes Google Events and returns structured JSON. Google Events aggregates from multiple event sources, so SerpAPI gives us a single, curated pipeline we can vet and maintain. `playwright`, on the other hand, can scrape directly from library event calendars (e.g., Seattle Public Library), city and county event pages, and independent bookstore listings—sources that may not appear in SerpAPI results. It handles JavaScript and dynamic content that `requests` cannot. However, `requests` cannot render JS or scrape sites that load content dynamically while `playwright` requires a headless browser, extra setup, and selectors that can break when page structure changes. Additionally, big sites like Meetup and Eventbrite prohibit scraping in their Terms of Service, which limits what `playwright` can do legally. Our main consideration was quality control and consistency. We wanted to be intentional about our sources, but selectively choosing our sources with `playwright` yielded very few events and inconsisttent formats across several websites, making data cleaning difficult. SerpAPI gives us a single pipeline to vet. More importantly, SerpAPI gave us significantly more events.

### Decision

With all these considerations in mind, we ultimately decided to stick with `requests` for SerpAPI calls. It keeps our events organized under a single format, our pipeline simple, and gives the users more options for events. 

### Drawbacks

The main drawback of `requests` is that it only retrieves raw HTTP responses and does not execute JavaScript or render dynamic content. As a result, `requests` cannot access content that loads after the initial page render (e.g., infinite scroll, lazy-loaded images, or AJAX-fetched data). It also cannot simulate user interactions such as clicking, scrolling, filling forms, or navigating login flows—so any site that gates content behind these actions is out of reach. We are therefore locked into APIs and static pages. If SerpAPI or Google Events changed their structure or rate limits, we would have limited alternatives without switching to a browser-based tool. Moreover, drawing from different sources that SerpAPI feeds us, we cannot guarantee that the event is a valid book event from a reliable source compared to SerpAPI where we can choose the websites we want to scrape from. 

### Demo

The notebook `requests_vs_playwright_demo.ipynb` compares both approaches. **Part 1** uses `data/scripts/get_seattle_bookclubs_data.py` to fetch events via SerpAPI (structured JSON). **Part 2** uses Playwright's async API to scrape five permitted sources (Secret Garden Books, Third Place Books, SPL, Seattle.gov, Elliott Bay). We avoid Meetup and Eventbrite due to their ToS. The notebook shows event counts and sample rows.

## scikit-learn vs Tensorflow

### Background

We are developing a hybrid, personalized book recommendation system to help users discover relevant books for a book club. The goal is to combine multiple data sources — Amazon book reviews, Amazon book meta data, library checkouts, and user interactions, to deliver recommendations that are accurate, diverse, and tailored to individual preferences. This use case requires a Python library capable of handling structured tabular data, computing similarity metrics, and building simple models for content-based and collaborative filtering.

### Description

`scikit-learn` is user-friendly, with a simple API that requires minimal boilerplate. It excels on tabular and structured data, and integrates seamlessly with the Python data stack, including `pandas` and `NumPy`. `TensorFlow`, on the other hand, is designed for deep learning and handles complex or unstructured data such as images, text, or sequences. While its high-level Keras API is relatively easy to use, the low-level `TensorFlow` framework is more complex and requires additional preprocessing for tabular datasets. `TensorFlow` also scales efficiently to large datasets with GPU acceleration. Both libraries benefit from strong communities, extensive documentation, and numerous examples, but `scikit-learn` is better suited for straightforward tabular data workflows, whereas `TensorFlow` provides flexibility for more complex models and large-scale training.

### Comparison

In terms of ease of use, `scikit-learn` has a very simple API and minimal boilerplate, while `TensorFlow` requires more setup, especially if using its low-level API. For data suitability, `scikit-learn` is optimal for tabular and structured data, whereas `TensorFlow` shines with unstructured or sequential data such as images, text, or time series. Integration with existing Python tools is simpler in `scikit-learn`, while `TensorFlow` often requires additional preprocessing steps. Both have strong community support, extensive documentation, and numerous tutorials.

### Decision

We ultimately decided to choose `scikit-learn` instead of `TensorFlow` because of its simplicity, ease of use, and clear documentation. Although `TensorFlow` is typically considered better for large data sets, we are primarily interested in calculating the cosine similarity between books based on user review histories. While this is a massive data set (571.54M rows) filtering to only “positive reviews” reduces the size dramatically and furthermore the matrix we construct of book cosine similarity is extremely sparse (only about 0.005% non-zero rows). For this reason, `scikit-learn` is sufficient for our uses given we are careful to store our matrix as a sparse matrix. Additionally, as our Amazon reviews data set is static we only do this calculation once so the speed of this operation is not that important (as long as it runs in a reasonable amount of time). For our purpose, it is much more important that the output dataset is compact and optimally structured for later use in our recommender. 


### Drawbacks

As we mentioned above, `scikit-learn` is best for doing traditional machine learning tasks on small to medium sized data sets. While `scikit-learn` is sufficient for the complexity of machine learning tasks we want to do (calculating similarity scores and fitting a simple linear regression), our data sets are very large making `scikit-learn` potentially slower than `TensorFlow` due to its lack of optimization for large datasets. To address this issue we are careful to use sparse matrix operations to dramatically reduce the size of the data we are working with. However, if we want to use the full 571.54M row data set or possibly a larger updated dataset in the future we may need to reimplement our code using `TensorFlow` for better performance and scaling. 
