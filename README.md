# FPL Analytics

- uv for dependencies management
- motherduck to persist data
- dlt to load data
- dbt to transform data
- visualise in streamlit
- modal to host the webapp
- github action to schedule ELT runs

## Tasks 

#### Backend

- [x] Initialise project
    - git & uv
- [x] Load fpl data
    - Requires motherduck account, token and database
- [x] Load football-data data (fixtures and standings)
    - Requires API key
- [x] Transform data with dbt models
- [ ] Create github action to run dlt and dbt

#### Frontend

- [ ] Create streamlit app
- [ ] Host streamlit app on modal
