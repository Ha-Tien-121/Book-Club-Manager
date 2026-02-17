# Milestones
This section details a preliminary timeline for the project. It includes milestones with lists of tasks in priority order, expected deliverables, and their deadlines.

## Milestone 1: Data Cleaning and Processing
**Deadline**: Feb 20, 2026

### Objective
Prepare datasets needed for recommendation systems and frontend and backend development

### Tasks
* Set up Python backend skeleton
* Create virtual environment
* Choose framework
* Add basic server + test route
* Aggregate SPL Checkouts dataset
* Convert SPL Catalog to availability mapping
* Parse Events dataset into structured fields
* Save cleaned datasets to `/data/processed/`

### Deliverables
* Running local backend
* Cleaned datasets + reproducible scripts

## Milestone 2: Feature Selection + Recommender Systems
**Deadline**: Feb 27, 2026

### Objective
Build working recommendation prototypes for books and events, merge datasets and finalize features for backend usage

### Tasks
* Merge SPL availability with Books dataset
* Prepare Amazon Reviews dataset
* Create reader preference features
* Implement recommendation scoring function
* Test recommender systems
### Deliverables
* Finalized datasets (e.g. unified books dataset)
* Working recommender prototypes

## Milestone 3: Backend Infrastructure
**Deadline**: Mar 6, 2026

### Objective
Deploy backend infrastructure to store data and serve recommendations through an API.

### Tasks
* Set up AWS backend environment
* Create database schema
* Connect processing pipeline to database
* Store cleaned data in database
* Add API routes for recommendations

### Deliverables
* AWS backend deployed
* Database connected
* Backend endpoints working

## Milestone 4: UI Integration
**Deadline**: Mar 6, 2026

### Objective
Build a user interface that allows users to interact with website features, including recommendations, events, books read, etc.

### Tasks
* Build recommendation UI pages
* Add filters and search
* Connect UI to backend API
* Run usability tests

### Deliverables
* Functional UI prototype connected to backend

## Milestone 5: Website Deployment
**Deadline**: Mar 16, 2026

### Objective
Deliver a fully integrated and stable system that users can access online.

### Tasks
* Integrate frontend, backend, and database
* Run end-to-end tests
* Fix bugs and performance issues
* Deploy website
* Write documentation

### Deliverables
* Live website
* Final documentation

## Milestone 6: Final Presentation  
**Deadline**: Mar 17, 2026

### Objective
Present the system design, implementation, and results to class

### Tasks
* Prepare slides
* Create demo workflow
* Assign speaking roles
* Rehearse presentation

### Deliverables
* Final slides
* Demo-ready version
