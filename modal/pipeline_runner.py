import modal
import datetime
from pathlib import Path

"""
Test locally with 
uv run modal serve modal/pipeline_runner.py
Deploy with (after any changes in extract_load/ or transform/)
uv run modal deploy modal/pipeline_runner.py
"""

PROJECT_ROOT = Path(__file__).parent.parent

app = modal.App("fpl-analytics")

image = (
    modal.Image.debian_slim()
    .pip_install("dlt[motherduck]", "dbt-duckdb", "httpx")
    .add_local_dir(PROJECT_ROOT / "extract_load", "/root/extract_load")
    .add_local_dir(PROJECT_ROOT / "transform", "/root/transform")
    .add_local_file(PROJECT_ROOT / "dbt_project.yml", "/root/dbt_project.yml")  # ADD THIS
    .add_local_file(PROJECT_ROOT / "profiles.yml", "/root/profiles.yml")  # ADD THIS if you have it
)


@app.function(
    schedule=modal.Cron('30 4 * * *'),
    secrets=[modal.Secret.from_name("motherduck-secret"), 
             modal.Secret.from_name("football-data-api-key")],
    retries=2,
    timeout=300,
    image=image,
)
def run_pipeline():
    import sys
    sys.path.insert(0, "/root")
    
    from extract_load.loaders import load_fpl, load_from_football_data
    from transform.transformer import run_dbt_transformations
    
    print(f"Running pipeline ({datetime.datetime.now()})... ")

    results = {}
    
    try:
        print("📥 Loading FPL player data...")
        fpl_result = load_fpl()
        results['fpl'] = fpl_result
        print(f"✅ {fpl_result}")
        
        print("📥 Loading fixtures...")
        fixtures_result = load_from_football_data()
        results['fixtures'] = fixtures_result
        print(f"✅ {fixtures_result}")
        
        print("🔄 Running dbt transformations...")
        dbt_result = run_dbt_transformations()
        results['dbt'] = dbt_result
        print(f"✅ {dbt_result}")
        
        return results
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise