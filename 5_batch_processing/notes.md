# Week 5: Batch Processing

## 5.1.1 - Introduction to Batch processing

### Types of Data Processing
| Batch | Streaming |
| - | - |
| Take all data for date, single job processes, produces another data set | processed in real time as it is created |

Batch Jobs
- Weekly/Daily/Hourly
    - Can be smaller or larger time, but not as typical

Tech
- Python scripts
    - Can run in kubernetes, aws batch, etc
- SQL for transformations
- Spark: tech for batch jobs

Workflow
1. Data Lake (CSV)
2. Python
3. SQL
4. Spark
...

### Advantages/Disadvantages of Batch
Advantages
- Easy to manage (workflow tools)
- Easy to retry
- Easy to scale
Disadvantages
- Delay

## 5.1.2 - Introduction to Spark

A multilanguage data processing *Engine*

Generally reading in Scala, but wrappers for Python, R, other envs: Python called *pyspark*.

Can be used for streaming, generally batch jobs

### Usage

- When data is in Data Lake (S3, GCS)
- Same things you'd use SQL for
    - In a DWH, would use SQL
    - Hive or Presto (or External Tables) can run right on DL
    - If you can express batch job as SQL- go for it

Typical Workflow
1. Raw Data
2. Data Lake
3. SQL/Athena transformations
4. Spark transformations
5. Python (ML) job

**If you can express it in SQL, go with SQL. If not, use spark.**

export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

## 5.3.1 - First Look at Spark/PySpark

