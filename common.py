import datetime
import prefect
from prefect import task

@task(log_stdout=True)
def get_start_time_task():
    schedule_time = prefect.context.get("scheduled_start_time")
    if schedule_time:
        return schedule_time
    else:
        datetime.datetime.now()