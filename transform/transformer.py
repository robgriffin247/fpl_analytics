import subprocess
import os
from pathlib import Path

def run_dbt_transformations(profiles_dir="."):
    """Run dbt transformations"""
    
    # Get absolute paths for debugging
    cwd = Path("./transform").resolve()
    dbt_project_dir = Path("/root")  # Not /root/transform
    profiles_dir = Path("/root")  # Or wherever your profiles.yml is
       
    print(f"Running dbt from: {cwd}")

    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", str(profiles_dir)],
        cwd=str(dbt_project_dir),
        capture_output=True,
        text=True,
        env=os.environ.copy()
    )
    
    # Print both stdout and stderr for debugging
    print(f"Return code: {result.returncode}")
    print(f"STDOUT:\n{result.stdout}")
    print(f"STDERR:\n{result.stderr}")
    
    if result.returncode != 0:
        raise Exception(
            f"dbt failed with return code {result.returncode}\n"
            f"STDOUT: {result.stdout}\n"
            f"STDERR: {result.stderr}"
        )
    
    # Parse dbt output for stats
    success_count = result.stdout.count("OK created")
    
    return {
        "status": "success",
        "models_built": success_count,
        "output": result.stdout
    }

if __name__ == "__main__":
    print(run_dbt_transformations())