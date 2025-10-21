import modal
import datetime

app = modal.App("test-app")

@app.function(schedule=modal.Cron('45 19 21 * *'))
def print_timestamp():
    print(f"The time is now {datetime.datetime.now()}")