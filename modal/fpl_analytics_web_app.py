import modal
import shlex
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

app_local_path = PROJECT_ROOT / "ui" / "app.py"
app_remote_path = "/root/ui/app.py"

image = (
    modal.Image.debian_slim()
    .pip_install("duckdb", "streamlit", "plotly-express", "polars", "emoji")
    .add_local_dir(PROJECT_ROOT / "ui", "/root/ui")
    .add_local_file(app_local_path, app_remote_path)
    .add_local_file(PROJECT_ROOT / "ui/inputs.py", "/root/ui/inputs.py")
    .add_local_file(PROJECT_ROOT / "ui/loaders.py", "/root/ui/loaders.py")
    .add_local_file(PROJECT_ROOT / "ui/utils.py", "/root/ui/utils.py")
    .add_local_file(PROJECT_ROOT / "ui/visuals.py", "/root/ui/visuals.py")
    # .add_local_file(PROJECT_ROOT / ".streamlit/secrets.toml", "/root/.streamlit/secrets.toml")
)

app = modal.App("fpl-analytics-web-app", image=image)


@app.function(secrets=[modal.Secret.from_name("fpl-analytics-secrets")])
@modal.concurrent(max_inputs=20)
@modal.web_server(8000)
def host_web_app():
    # target = shlex.quote(str(app_local_path))
    cmd = f"streamlit run {app_remote_path} --server.port 8000 --server.enableCORS=false --server.enableXsrfProtection=false"
    subprocess.Popen(cmd, shell=True)
