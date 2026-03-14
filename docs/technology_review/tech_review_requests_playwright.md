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