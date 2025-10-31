import modal
import datetime
from pathlib import Path
from extract_load.loaders import load_fpl, load_football_data
from transform.transformer import run_dbt_transformations
import sys

PROJECT_ROOT = Path(__file__).parent.parent

app = modal.App("fpl-analytics-backend")

image = (
    modal.Image.debian_slim()
    .pip_install("dlt[motherduck]", "dbt-duckdb", "httpx")
    .add_local_dir(PROJECT_ROOT / "extract_load", "/root/extract_load")
    .add_local_dir(PROJECT_ROOT / "transform", "/root/transform")
    .add_local_file(PROJECT_ROOT / "dbt_project.yml", "/root/dbt_project.yml")
    .add_local_file(PROJECT_ROOT / "profiles.yml", "/root/profiles.yml")
)

@app.function(
    schedule=modal.Cron('30 4 * * *'),
    secrets=[modal.Secret.from_name("fpl-analytics-secrets")],
    retries=2,
    timeout=300,
    image=image,
)
def run_pipeline():
    sys.path.insert(0, "/root")
        
    print(f"Running pipeline ({datetime.datetime.now()})... ")

    results = {}
    
    try:
        print("📥 Loading FPL player data...")
        fpl_result = load_fpl()
        results['fpl'] = fpl_result
        print(f"✅ {fpl_result}")
        
        print("📥 Loading football-data...")
        football_data_result = load_football_data()
        results['football_data'] = football_data_result
        print(f"✅ {football_data_result}")
        
        print("🔄 Running dbt transformations...")
        dbt_result = run_dbt_transformations()
        results['dbt'] = dbt_result
        print(f"✅ {dbt_result}")
        
        return results
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise


@app.function(
    secrets=[modal.Secret.from_name("fpl-analytics-secrets")],
    retries=2,
    timeout=300,
    image=image,
)
def load_fpl_job():
    sys.path.insert(0, "/root")
        
    results = {}

    try:
        print("📥 Loading FPL player data...")
        fpl_result = load_fpl()
        results['fpl'] = fpl_result
        print(f"✅ {fpl_result}")

        return results

    except Exception as e:
        print(f"❌ Run failed: {e}")
        raise



@app.function(
    secrets=[modal.Secret.from_name("fpl-analytics-secrets")],
    retries=2,
    timeout=300,
    image=image,
)
def load_footall_data_job():
    sys.path.insert(0, "/root")
        
    results = {}

    try:
        print("📥 Loading football-data...")
        football_data_result = load_football_data()
        results['football_data'] = football_data_result
        print(f"✅ {football_data_result}")

        return results

    except Exception as e:
        print(f"❌ Run failed: {e}")
        raise


    
@app.function(
    secrets=[modal.Secret.from_name("fpl-analytics-secrets")],
    retries=2,
    timeout=300,
    image=image,
)
def dbt_transform_job():
    sys.path.insert(0, "/root")
        
    results = {}

    try:
        print("🔄 Running dbt transformations...")
        dbt_result = run_dbt_transformations()
        results['dbt'] = dbt_result
        print(f"✅ {dbt_result}")

        return results

    except Exception as e:
        print(f"❌ Run failed: {e}")
        raise