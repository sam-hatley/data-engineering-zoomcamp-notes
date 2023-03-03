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

# Week 5 Homework

>For this homework we will be using the FHVHV 2021-06 data found here. [FHVHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz )


## Question 1: 

>**Install Spark and PySpark** 
>- Install Spark
>- Run PySpark
>- Create a local spark session
>- Execute spark.version.

>What's the output?
- 3.3.2
- 2.1.4
- 1.2.3
- 5.4
</br></br>


### Question 2: 

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br> 
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>


- 2MB
- 24MB
- 100MB
- 250MB
</br></br>


### Question 3: 

**Count records**  

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- 308,164
- 12,856
- 452,470
- 50,982
</br></br>


### Question 4: 

**Longest trip for each day**  

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- 66.87 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours
</br></br>

### Question 5: 

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- 80
- 443
- 4040
- 8080
</br></br>


### Question 6: 

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- East Chelsea
- Astoria
- Union Sq
- Crown Heights North
</br></br>




## Submitting the solutions

* Form for submitting: https://forms.gle/EcSvDs6vp64gcGuD8
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 06 March (Monday), 22:00 CET


## Solution

We will publish the solution here
