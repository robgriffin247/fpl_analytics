import modal
import datetime
import os 

app = modal.App("test-app")

@app.function(
        schedule=modal.Cron('*/1 * * * *'),
        secrets=[modal.Secret.from_name("motherduck-secret")],
    )
def run_pipeline():
    print(f"The time is now {datetime.datetime.now()}: {os.getenv("MOTHERDUCK_TOKEN")[0:10]}")

    