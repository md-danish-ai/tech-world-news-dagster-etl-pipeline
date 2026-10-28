import os
from datetime import datetime, timezone
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    run_failure_sensor,
    RunFailureSensorContext,
)
from src.assets import bronze, silver, gold
from src.resources.hn_api import HackerNewsResource
from src.utils.helpers import IST

# 1. Load all assets from bronze, silver, and gold modules
all_assets = [
    *load_assets_from_modules([bronze, silver, gold]),
]

# 2. Define the Pipeline Job
hackernews_etl_job = define_asset_job(
    name="hackernews_etl_job",
    selection=AssetSelection.all(),
    description="End-to-end Medallion ETL pipeline: Bronze Ingestion -> Silver Cleaning -> Gold Analytics.",
)

# 3. Schedule: Triggers the pipeline automatically EVERY 2 HOURS
hackernews_2hour_schedule = ScheduleDefinition(
    job=hackernews_etl_job,
    cron_schedule="0 */2 * * *",
    description="Automatically runs the Hacker News ETL pipeline every 2 hours.",
)

# 4. Failure Alert Sensor: Logs any pipeline failure to a dedicated audit file
@run_failure_sensor
def pipeline_failure_logger_sensor(context: RunFailureSensorContext):
    """Automatically intercepts any pipeline failure and logs Run ID, job name, and error stack trace."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "pipeline_failures.log")

    now_ist = datetime.now(timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    error_snippet = context.failure_event.message if context.failure_event else "Unknown error"

    log_entry = (
        f"\n{'='*75}\n"
        f"🚨 PIPELINE FAILURE DETECTED\n"
        f"Timestamp:  {now_ist}\n"
        f"Job Name:   {context.dagster_run.job_name}\n"
        f"Run ID:     {context.dagster_run.run_id}\n"
        f"Error Details:\n{error_snippet}\n"
        f"{'='*75}\n"
    )

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)


# 5. Register everything cleanly in Dagster
defs = Definitions(
    assets=all_assets,
    jobs=[hackernews_etl_job],
    schedules=[hackernews_2hour_schedule],
    sensors=[pipeline_failure_logger_sensor],  # <-- Attached Failure Sensor!
    resources={
        "hn_api": HackerNewsResource(),
    },
)
