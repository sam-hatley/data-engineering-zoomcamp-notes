# Week 5 Notes

Notes from home stopped at 5.2.1, I think.

## 5.2 Spark SQL and DataFrames

### 5.3.1 - First Look at Spark/PySpark

Most notes available for this in [init.ipynb](init.ipynb)

### 5.3.2 - Spark DataFrames

Most notes available for this in [init.ipynb](init.ipynb)

### 5.3.3 - (Optional) Preparing Yellow and Green Taxi Data

Simple [bash script](prep/dl.sh) to dl data:

```sh
TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`
    LOCAL_PREFIX="../data/raw/${TAXI_TYPE}/${YEAR}"
    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    mkdir -p ${LOCAL_PREFIX}
    wget ${URL} -O ${LOCAL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz
done
```

Further notes in [taxi_schema.ipynb](prep/taxi_schema.ipynb).


### 5.3.4 - SQL with Spark

Notes in [06_spark_sql.ipynb](06_spark_sql.ipynb).

## 5.4 Spark Internals

### 5.4.1 - Anatomy of a Spark Cluster

#### Spark Cluster

**Local Cluster** - everything on the same computer, as in the cases above:
```py
spark = SparkSession.builder \
    .master("local[*]") \
    ...
```
The code above specifying that this is a local build with the `.master("local[*]")` command: we're creating a local cluster

**Cluster** - Has a master with a web UI on port 4040 which commands several executors, which handle jobs given by the master. Master should be running continually, executors can drop out.

When processing a partitioned dataframe, executors will process the files one by one from the data lake in S3/GCS.

Hadoop/HDFS allows us to store the files directly on executors with some redundancy: instead of downloading data to a machine, you just download the code to process data locally. The concept is called *data locality*. This is less popular today, as most cloud solutions are within the same data center- download speeds are relatively low, and the additional complexity from Hadoop/HDFS only provides a marginal speed improvement.

### 5.4.2 - GroupBy in Spark

Notes in [07_groupby_join.ipynb](07_groupby_join.ipynb).

### 5.4.3 - Joins in Spark

Notes in [07_groupby_join.ipynb](07_groupby_join.ipynb).

### 5.5.1 - (Optional) Operations on Spark Resilient Distributed Datasets (RDDs)

[Come back to this later](https://www.youtube.com/watch?v=Bdu-xIrF3OM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=52)

### 5.5.2 - (Optional) Spark RDD mapPartition

Come back to this later

### 5.6.1 - Connecting to Google Cloud Storage

[See 5.6.1_GCS.ipynb](5.6.1_GCS.ipynb)

### 5.6.2 - Creating a Local Spark Cluster



#### Joining two large tables

#### Merge sort join

#### Joining one large and one small table

#### Broadcasting


## Submitting the solutions

* Form for submitting: https://forms.gle/EcSvDs6vp64gcGuD8
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 06 March (Monday), 22:00 CET


## Solution

We will publish the solution here
