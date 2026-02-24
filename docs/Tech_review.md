# Technology Review
## Requests vs. Selenium vs. Playwright

### Background

We plan on calling SerpAPI for event listings (aka when, where, what, and links) for book clubs. We plan to use a Python library to communicate with APIs over HTTP, specifically, downloading data files via URLs. To do this, we are entertaining three Python libraries: `requests`, `selenium`, and `playwright`.

### Description

The `requests` Python library is a lightweight HTTP client for REST/JSON APIs, such as SerpAPI in our case, and downloading CSV/JSON over HTTP. The `selenium` library drives a full browser like Chrome or Firefox to scrape data, typically suited for sites that need JS rendering or complex user interactions. Similarly, the `playwright` library is a more modern and faster headless browser automation for JS-heavy pages and network interception. 

### Comparison

For our SerpAPI workflow, `requests` is more straightforward as it speaks HTTP/JSON directly, stays fast and lightweight, and avoids any browser overhead. Additionally, `selenium` and `playwright` both initiate browsers, which require extra code and setup. `selenium` is also slower and can easily break if the page or the HTML structure changes. Although `playwright` is somewhat quicker and more reliable than `selenium`, it’d still be a browser stack overkill when the data already comes as structured JSON from an API. Most importantly, using `selenium` or `playwright` to scrape sites like Meetup would violate their Terms of Service and potentially run into issues with anti-bot defenses sites may have. On the other hand, using SerpAPI through `requests` is authorized. 

### Decision

With all these considerations in mind, we ultimately decided to stick with `requests` for SerpAPI calls to stay within the allowed usage. It is the simplest, fastest, and least brittle for API endpoints. 

### Drawbacks

Some drawbacks for `requests` are that it cannot execute JavaScript or render dynamic pages since it only retrieves the raw HTTP response. This makes it bad for sites where content loads after page render or requires user interactions such as clicking, scrolling, or navigating login flows. In comparison, `selenium` and `playwright` are more effective at scraping interactive or JavaScript-heavy web applications when no direct API is available. 


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
