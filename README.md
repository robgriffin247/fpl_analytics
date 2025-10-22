# FPL Analytics

## Aim

Create a front-end web app that displays data relating to fantasy football, where users can filter, visualise and explore data on players and fixtures. Create a back-end that extracts and loads data from multiple sources, transforms the data into production ready data, and automatically collects the latest data every day. Data models have been designed to persist data for each gameweek.

## Tech Stack

- git for version control
- uv for package management
- motherduck to persist data
- dlt to extract/load data
- dbt to transform data
- modal to orchestrate dlt and dbt runs
- streamlit for the UI
- modal to host the webapp
- github workflows
    - redeploy the pipeline to modal on changes to ``./modal``, ``./extract_load``, ``./transform`` and ``pyproject.toml``


## Run this project

- Prerequisites:
    - Motherduck account and [token](https://app.motherduck.com/settings/tokens)
    - Account and API key for [football-data.org](https://www.football-data.org/documentation/quickstart/)

1.  Clone

    ```
    git clone git@github.com:robgriffin247/fpl_analytics
    cd fpl_analytics
    ```
    
1. Set up secrets/tokens
    1. Run ``cp .env_template .env``
    1. Add values to the ``.env``
    1. Run ``direnv allow``

1. Install dependencies

    ```
    uv sync
    ```

1. Run/deploy the pipeline (note the pipeline_runner.py script auto-redeploys as needed via github workflows)

    ```
    # modal run or serve to test; run ignores cron schedule
    # modal deply to update the deployed app
    uv run modal run modal/pipeline_runner.py
    ```

1. Run the streamlit app locally

    ```
    uv run streamlit run ui/app.py
    ```

## Tasks/Ideas

- [ ] Host UI on modal
- [x] Github action/webhook (whatever the right tool is called) to deploy the modal app(s) on changes to main (if the relevant files change)
- [ ] Run black linter before commit to main