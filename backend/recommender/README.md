# Recommender Module

This folder contains the recommendation logic used by the Bookish backend.

The system provides:
- Book club event recommendations (implemented)
- Book recommendations (in progress)

The recommender receives user preferences from the backend API and returns
a ranked list of results based on relevance.

---

## Structure

---


## Events Recommender (In Progress)

The events recommender suggests book club events using data from sources
such as SerpAPI and library datasets.

### Inputs
- User genres or keywords  
- User location (optional)  
- Event dataset with fields like title, description, time, and location  

### Outputs
A ranked list of events including title, time, location, and score.

Results are returned through the backend endpoint:

POST /recommendations/events

### Ranking Strategy
Events are ranked using a simple priority order:

1. Topic match + upcoming time  
2. Location match + time  
3. Location match + topic  

This approach favors events that are both relevant and practical to attend.

---

## Books Recommender (In Progress)

A book recommendation module is being developed separately. It will use
metadata and review datasets to suggest books tailored to user interests
and club activity.

Details will be added as this component is implemented.