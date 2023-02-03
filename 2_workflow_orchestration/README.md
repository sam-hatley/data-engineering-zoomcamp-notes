# Week 2: Workflow Orchestration

## Homework

### 1. Load January 2020 Data

>Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.
>
>How many rows does that dataset have?

```
447,770
```

This just required a modification of [parameterized_flow.py](parameterized_flow.py), which was already built off the top of `etl_web_to_gcs.py`. 

The script can be modified to import green taxi data by accessing the "Deployments" tab in Prefect Orion, selecting the deployment ("Parameterized ETL", in this case), selecting "Parameters", and editing those to `months = 1, year = 2020, color = "green"`. However, while the yellow taxi data uses `tpep_pickup_datetime` and `tpep_dropoff_datetime`, the green taxi data uses `lpep_pickup_datetime` and `lpep_dropoff_datetime`, which broke the script setting those columns to datetime. Turning the first portion of the row into a variable fixes this issue, as in the below:

```py
@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == "yellow":
        dtstr = "tpep"
    elif color == "green":
        dtstr = "lpep"

    df[f"{dtstr}_pickup_datetime"] = pd.to_datetime(df[f"{dtstr}_pickup_datetime"])
    df[f"{dtstr}_dropoff_datetime"] = pd.to_datetime(df[f"{dtstr}_dropoff_datetime"])
    ...
```

The relevant line in the output with the solution comes from the logs built into the program earlier:

```
10:51:26.900 | INFO    | Task run 'clean-2c6af9f6-0' - Before no passengers: 447770
```

### 2. Scheduling with Cron

>Cron is a common scheduling specification for workflows.
>
>Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

```
0 5 1 * *
```

E.g., cron specifies minute, hour, day, month, and day-of-week: the above signifies at minute 0, hour 05:00, on the 1st of the month, any month, any day of the week.

### 3. Loading data into BigQuery

>Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).
>
>The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.
>
>Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.
>
>Make any other necessary changes to the code for it to function as required.
>
>Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).
>
>Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

```
14,851,920
```

Not much was needed to change the [original file](etl_gcs_to_bq.py): ultimately, I needed to remove the `transform` function from the main flow, use a for-loop to manage multiple months, load the df within the main flow, and count/report the rows processed. The code required looks like this (the full file is [here](homework/etl_gcs_to_bq.py).):

```py
@flow(log_prints=True)
def etl_gcs_to_bq(color: str = "yellow", year: int = 2019, months: list = [2, 3]) -> None:
    """Main ETL flow to load data into BigQuery"""

    row_count = 0
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = pd.read_parquet(path)
        write_bq(df)
        row_count += len(df)
    
    print(f"{row_count} rows processed.")
```

From which the relevant output was:

```
14:35:23.510 | INFO    | Flow run 'literate-angelfish' - 14851920 rows processed.
```

### 4. Github Storage Block

>Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.
>
>Note that you will have to push your code to GitHub, Prefect will not push it for you.
>
>Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.
>
>How many rows were processed by the script?

```
88,605
```

This also required the same updates from [question 1](#1-load-january-2020-data) to process green taxi data. I was able to build the block using this particular folder of the repo with the below code:
```py
from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/sam-hatley/data-engineering-zoomcamp-notes.git"
)
block.get_directory("2_workflow_orchestration/homework")  # specify a subfolder of repo
block.save("dezoomcamp-git")
```

This was then built with the command:
```bash
prefect deployment build 2_workflow_orchestration/homework/etl_web_to_gcs_hw.py:etl_web_to_gcs --name etl_web_to_gcs --tag dezoomcamp-git -sb github/dezoomcamp-git -a
```

...and run with `prefect run etl-web-to-gcs/etl-web-to-gcs`. The code was modified to skip out on actually uploading to GCS- it works just fine if you uncomment the last two lines in `etl_web_to_gcs()`. The file used was [etl_web_to_gcs_hw.py](homework/etl_web_to_gcs_hw.py).


### 5. Email or Slack notifications

>It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.
>
>The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur.
>
>Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up.
>
>Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.
>
>Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook.
>
>Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days.
>
>In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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
>
>Test the functionality.
>
>Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create.
>
>How many rows were processed by the script?

```
125,268
377,922
728,390
514,392
```

### 6. Secrets

>Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

```
8
```

This feels superfluous.

https://forms.gle/PY8mBEGXJ1RvmTM97