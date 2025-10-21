import modal
import datetime
import os 
from extract_load.loaders import load_fpl, load_from_football_data
from transform.transformer import run_dbt_transformations
app = modal.App("test-app")
image = (
    modal.Image.debian_slim()
    .pip_install(
        "dlt[motherduck]",
        "dbt-duckdb",
        "requests"
    )
    # Use pip_install_from_pyproject if you have pyproject.toml
    .pip_install_from_pyproject("pyproject.toml")
)

@app.function(
        schedule=modal.Cron('*/12 * * * *'),
        secrets=[modal.Secret.from_name("motherduck-secret")],
        retries=2,
        timeout=300,
        image=image,
    )
def run_pipeline():
    print(f"Running pipeline ({datetime.datetime.now()})... ")

    results = {}
    
    try:
        # Step 1: Extract FPL data
        print("📥 Loading FPL player data...")
        fpl_result = load_fpl()
        results['fpl'] = fpl_result
        print(f"✅ {fpl_result}")
        
        # Step 2: Extract fixtures
        print("📥 Loading fixtures...")
        fixtures_result = load_from_football_data()
        results['fixtures'] = fixtures_result
        print(f"✅ {fixtures_result}")
        
        # Step 3: Run dbt transformations
        print("🔄 Running dbt transformations...")
        dbt_result = run_dbt_transformations()
        results['dbt'] = dbt_result
        print(f"✅ {dbt_result}")
        
        return results
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise
