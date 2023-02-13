## Week 3 Homework
><b><u>Important Note:</b></u> <p>You can load the data however you would like, but keep the files in .GZ Format. 
>If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
><u>NOTE:</u> You can use the CSV option for the GZ files when creating an External Table</br>

><b>SETUP:</b></br>
>Create an external table using the fhv 2019 data. </br>
>Create a materialized table using the fhv 2019 data (do not partition or cluster this table). </br>
>Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

Setup was done like so:
1. Files were imported using Prefect to mass-download from github into GCP via [load_to_bucket.py](load_to_bucket.py)
2. I created an external table using the command below
```sql
CREATE OR REPLACE EXTERNAL TABLE dezoomcamp-1.dezoomcamp.external_tripdata
OPTIONS (
    format = 'CSV',
    uris = ['https://storage.cloud.google.com/dezoomcamp1/fhv/fhv_tripdata_2019-*.csv.gz']
);
```
3. I created a materialized table using the command below
```sql
CREATE TABLE dezoomcamp-1.dezoomcamp.materialzed_tripdata AS 
  SELECT * FROM dezoomcamp-1.dezoomcamp.external_tripdata;
```

**NB: Code is presented below the questions**

## Question 1:
>What is the count for fhv vehicle records for year 2019?

- 43,244,696

```sql
SELECT COUNT(*) FROM `dezoomcamp-1.dezoomcamp.external_tripdata`;
```

## Question 2:
>Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the external table and the materialized table.</br> 
>What is the estimated amount of data that will be read when this query is executed on the External Table and the Materialized Table?

- 0 MB for the External Table and 317.94MB for the Materialized Table 

```sql
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dezoomcamp-1.dezoomcamp.materialzed_tripdata`;
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dezoomcamp-1.dezoomcamp.external_tripdata`;
```


## Question 3:
>How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- 717,748

```sql
SELECT COUNT(*) 
FROM `dezoomcamp-1.dezoomcamp.materialzed_tripdata`
WHERE PUlocationID is NULL
  AND DOlocationID is NULL;
```

## Question 4:
>What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- Partition by pickup_datetime Cluster on affiliated_base_number

Partitioning a table can greatly speed up a filter. Clustering can help when sorting:
>Clustered tables in BigQuery are tables that have a user-defined column sort order using clustered columns.
[https://cloud.google.com/bigquery/docs/clustered-tables](https://cloud.google.com/bigquery/docs/clustered-tables)

In this case, it makes more sense to partition by pickup_datetime and cluster by affiliated_base_number.

## Question 5:
>Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).</br> 
>Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

*Please* don't use a US date format wihout warning us! This was very confusing.

```sql
CREATE OR REPLACE TABLE dezoomcamp-1.dezoomcamp.materialzed_tripdata_partitioned
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
  SELECT * FROM dezoomcamp-1.dezoomcamp.materialzed_tripdata;

SELECT DISTINCT Affiliated_base_number
FROM dezoomcamp-1.dezoomcamp.materialzed_tripdata --and "_partitioned" in a second query
WHERE pickup_datetime >= "2019-03-01"
    AND pickup_datetime <= "2019-03-31";
```

## Question 6: 
>Where is the data stored in the External Table you created?

- GCP Bucket

Can verify by looking at Details > External Data Configuration, where the source URI(s) are printed. In my case, it is: `https://storage.cloud.google.com/dezoomcamp1/fhv/fhv_tripdata_2019-*.csv.gz`

## Question 7:
>It is best practice in Big Query to always cluster your data:
- False

Not in cases where tables are < 1GB, as per the lesson.

## (Not required) Question 8:
>A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and Materialized Table. 


>Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 

I honestly would've preferred this from the start. [Pipeline only lightly tested.](load_to_parquet.py)
 
## Submitting the solutions

* Form for submitting: https://forms.gle/rLdvQW2igsAT73HTA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 13 February (Monday), 22:00 CET
