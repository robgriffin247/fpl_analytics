# FPL Analytics

- git for version control
- uv for dependencies management
- motherduck to persist data
- dlt to extract/load data
- dbt to transform data
- modal to orchestrate dlt and dbt runs
- visualise in streamlit
- modal to host the webapp

## Tasks 

#### Backend

- [x] Initialise project
    - git & uv
- [x] Load fpl data
    - Requires motherduck account, token and database
- [x] Load football-data data (fixtures and standings)
    - Requires API key
- [x] Transform data with dbt models
- [ ] Orchestrate with Modal (consider Dagster cloud later)

#### Frontend

- [ ] Create streamlit app
- [ ] Host streamlit app on Modal

