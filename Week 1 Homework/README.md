# Week 1 Homework

>In this homework we'll prepare the environment and practice with Docker and SQL.

I've provided my code largely in the codeboxes below. If I've used a separate file, I've enclosed it in a [link](readme.md).


## Question 1. Knowing docker tags

>Run the command to get information on Docker 
>
>```docker --help```
>
>Now run the command to get help on the "docker build" command
>
>Which tag has the following text? - *Write the image ID to the file* 

```bash
docker build --help | grep ID
```

- `--iidfile string`


## Question 2. Understanding docker first run 

>Run docker with the python:3.9 image in an iterative mode and the entrypoint of bash.
>Now check the python modules that are installed ( use pip list). 
>How many python packages/modules are installed?

```bash
docker run -it --entrypoint=bash python:3.9
pip list
```

- 3

# Prepare Postgres

>Run Postgres and load data as shown in the videos
>We'll use the green taxi trips from January 2019:
>
>```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```
>
>You will also need the dataset with zones:
>
>```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```
>
>Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

[The jupyter notebook used in this portion of the assignment.](ingest.ipynb)


## Question 3. Count records 

>How many taxi trips were totally made on January 15?
>
>Tip: started and finished on 2019-01-15. 
>
>Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the >format timestamp (date and hour+min+sec) and not in date.

```sql
SELECT COUNT(*)
FROM green_taxi_data    
WHERE lpep_pickup_datetime >= '2019-01-15'
    AND lpep_dropoff_datetime < '2019-01-16';
```

- 20530

## Question 4. Largest trip for each day

>Which was the day with the largest trip distance
>Use the pick up time for your calculations.

```sql
SELECT lpep_pickup_datetime, trip_distance
FROM green_taxi_data
ORDER BY trip_distance DESC
LIMIT 10;
```

- 2019-01-15

## Question 5. The number of passengers

>In 2019-01-01 how many trips had 2 and 3 passengers?

```sql
SELECT (SELECT COUNT(*)
        FROM green_taxi_data
        WHERE passenger_count = 2
        AND lpep_pickup_datetime >= '2019-01-01'
        AND lpep_pickup_datetime < '2019-01-02') AS two_pass,
       (SELECT COUNT(*)
        FROM green_taxi_data
        WHERE passenger_count = 3
        AND lpep_pickup_datetime >= '2019-01-01'
        AND lpep_pickup_datetime < '2019-01-02') AS three_pass;
```
 
- 2: 1282 ; 3: 254


## Question 6. Largest tip

>For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
>We want the name of the zone, not the id.
>
>Note: it's not a typo, it's `tip` , not `trip`

Doing this the hard way

```bash
sudo wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

```python
df = pd.read_csv('taxi_zone_lookup.csv')
df.head(n=0).to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')
df.to_sql(name='taxi_zone_lookup', con=engine, if_exists='append')
```

```sql
WITH puzone AS(
        SELECT gt.index AS pu_index,
               gt.tip_amount AS tip_amount,
               pu."Zone" AS pu_zone
        FROM green_taxi_data AS gt
        INNER JOIN taxi_zone_lookup AS pu
        ON gt."PULocationID" = pu."LocationID"),
     dozone AS(
        SELECT gt.index AS do_index,
               doff."Zone" AS do_zone
        FROM green_taxi_data AS gt
        INNER JOIN taxi_zone_lookup AS doff
        ON gt."DOLocationID" = doff."LocationID")
SELECT puzone.pu_index,
       dozone.do_index,
       puzone.tip_amount,
       puzone.pu_zone,
       dozone.do_zone
FROM puzone
INNER JOIN dozone
ON puzone.pu_index = dozone.do_index
WHERE pu_zone = 'Astoria'
ORDER BY tip_amount DESC;
```

That's a tough one.

- Long Island City/Queens Plaza


# Part B

>In this homework we'll prepare the environment by creating resources in GCP with Terraform.
>
>In your VM on GCP install Terraform. Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform) to your VM.
>
>Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 1. Creating Resources

>After updating the main.tf and variable.tf files run:
>
>```
>terraform apply
>```
>
>Paste the output of this command into the homework submission form.

The output of the command is below:

```bash
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following
symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + labels                     = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "dezoomcamp-1"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + dataset {
              + target_types = (known after apply)

              + dataset {
                  + dataset_id = (known after apply)
                  + project_id = (known after apply)
                }
            }

          + routine {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + routine_id = (known after apply)
            }

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "dtc_data_lake_dezoomcamp-1"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }

      + website {
          + main_page_suffix = (known after apply)
          + not_found_page   = (known after apply)
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_bigquery_dataset.dataset: Creation complete after 1s [id=projects/dezoomcamp-1/datasets/trips_data_all]
google_storage_bucket.data-lake-bucket: Creation complete after 2s [id=dtc_data_lake_dezoomcamp-1]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```

To be fair, I'm unsure if this was the intended result, and perhaps you wanted the response if you only used the default permissions on the VM, which was:

```bash
google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
╷
│ Error: googleapi: Error 403: Access denied., forbidden
│
│   with google_storage_bucket.data-lake-bucket,
│   on main.tf line 19, in resource "google_storage_bucket" "data-lake-bucket":
│   19: resource "google_storage_bucket" "data-lake-bucket" {
│
╵
╷
│ Error: Error creating Dataset: googleapi: Error 403: Request had insufficient authentication scopes.
│ Details:
│ [
│   {
│     "@type": "type.googleapis.com/google.rpc.ErrorInfo",
│     "domain": "googleapis.com",
│     "metadata": {
│       "method": "google.cloud.bigquery.v2.DatasetService.InsertDataset",
│       "service": "bigquery.googleapis.com"
│     },
│     "reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT"
│   }
│ ]
│
│ More details:
│ Reason: insufficientPermissions, Message: Insufficient Permission
│
│
│   with google_bigquery_dataset.dataset,
│   on main.tf line 45, in resource "google_bigquery_dataset" "dataset":
│   45: resource "google_bigquery_dataset" "dataset" {
```

As the form asks for code, I'll provide the steps taken here for reference.

1. Install terraform as per [the instructions](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli).

```bash
$ sudo apt-get update && sudo apt-get install -y gnupg software-properties-common
$ wget -O- https://apt.releases.hashicorp.com/gpg | \
    gpg --dearmor | \
    sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
$ gpg --no-default-keyring \
    --keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg \
    --fingerprint
$ echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
    https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
    sudo tee /etc/apt/sources.list.d/hashicorp.list
$ sudo apt update && sudo apt-get install terraform
```

2. Grab the files required from the [course repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform)

```bash
$ mkdir ~/terraform && cd ~/terraform
$ wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/week_1_basics_n_setup/1_terraform_gcp/terraform/main.tf
$ wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/week_1_basics_n_setup/1_terraform_gcp/terraform/variables.tf
```

3. Create a json file with account credentials for later usage. You'll need to go to "Service Accounts", "Keys", and create a json key, which you can send over via the following:

```bash
$ gcloud config set project $PROJECT_NAME
$ gcloud compute scp $JSON_KEY $VM_NAME:$REMOTE_DIR --ssh-key-file ~/.ssh/$KEY_NAME
```

4. We'll set that json as a credential with the following:

```bash
echo $GOOGLE_APPLICATION_CREDENTIALS $PATH
```

And use those credentials to login to google with the appropriate permissions:

```bash
gcloud auth application-default login
```

5. Make it happen

Run terraform to init, plan, validate, and apply. Destroy once you're finished with the resources:

```bash
$ terraform init
$ terraform plan
$ terraform validate
$ terraform apply
$ terraform destroy
```