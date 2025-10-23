# FPL Analytics

## Overview

The aim of this project is to create an app allowing the visualisation and exploration of data from Fantasy Premier League. That data includes player stats, team stats, fixtures and standings.

The project requires a back-end that automatically and routinely extracts, loads and transforms data from multiple APIs into a cloud-database. It also requires a front-end web app UI allowing interaction with the data and visualisations.

#### Data Stack


- git for version control
- uv for package management
- motherduck to persist data
- dlt to extract/load data
- dbt to transform data
- modal to orchestrate dlt and dbt runs
- streamlit for the web app/UI
- modal to host the web app
- github workflows for continuous deployment

![alt text](datastack.png)

#### Design

The FPL API includes statistics on players ("elements") and teams. The Football-Data API includes data on league standings and fixtures. This data is extracted from the APIs and loaded to a database on Motherduck using dlt. 

Data is then transformed using dbt to create datasets ready for use in the UI. Data models have been designed to persist data for each gameweek, retaining the latest load per gameweek for each player, to allow tracking of player performance over the season (obt_players contains one row per player and gameweek; the raw fpl_analytics.fpl.elements contains one row per player and dlt load). 

A Modal app with a cron schedule is used to run the two dlt pipelines and the dbt transformations every day. Data can be explored via the [motherduck web UI](https://app.motherduck.com/). When changes are committed to main, a Github workflow triggers a redeployment of the pipeline runner app if there have been any changes to files relating to the Modal pipeline runner app, dlt pipelines, dbt transformations or project dependencies.

Data is visualised in a Streamlit app (under construction). This will be hosted via Modal and have a similar deployment workflow.

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

1. Install python and dependencies

    ```
    uv python install
    uv sync
    ```

1. Run/deploy the pipeline (note the pipeline_runner.py script auto-redeploys as needed via github workflows)

    ```
    # modal run or serve to test; run ignores cron schedule
    # modal deploy to update the deployed app
    # run specific functions with e.g. ...pipeline_runner.py::dbt_only
    uv run modal run modal/pipeline_runner.py
    ```

1. Run the streamlit app locally

    ```
    uv run streamlit run ui/app.py
    ```

## Tasks/Ideas

- [ ] Develop UI
- [ ] Host UI on modal
- [ ] Linting
- [ ] Workflow to deploy webapp
- [ ] Slack/discord/email notice if pipeline fails