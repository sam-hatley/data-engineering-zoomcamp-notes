# Data Engineering Zoomcamp: Week 3
## 3.1.1 - Data Warehouse and BigQuery

OLAP vs OLTP

- **OLAP**: Online Analytical Processing
- **OLTP**: Online Transaction Processing 

| | OLTP | OLAP |
|-|-|-|
| Purpose | Control and run essential business operations in real time | Plan, solve problems, support decisions, discover hidden insights |
| Data updates | Short, fast, initiated by user | Periodically refreshed with scheduled, long-running batch jobs |
| DB design | Normalized for efficiency | Denormalized for analysis |
| Space requirements | Generally small if historical data is archived | Generally large due to aggregating large datasets |
| Backup and recovery | Regular backups required to ensure business continuity/meet legal/governance requirements | Lost data can be reloadted from OLTP DB as needed in lieu of regular backups |
| Productivity | Increases productivitiy of end users | Increases producivity of business mgmt, data analysts, executives |
| Data view | Day-to-day business transactions | Multi-dimensional view of enterprise data |
| User examples | Customer-facing personnel, clerks, online shoppers | Knowledge workers as above |

### Data Warehouses

- OLAP
- Used for reporting/data analysis

### BigQuery

- Serverless data warehouse
- Software as well as infrastructure
- Built-in features:
    - ML
    - Geospatial analysis
    - BI
- Very scalable
- Pricing
    - On-demand pricing: 1 TB processed is ~$5
    - Flat rate: 100 slots is $2,000/month = 400 TB

### External Tables in BigQuery

External tables store their metadata and schema in BQ storage.

Can create an external table from a dataset, i.e.:

```sql
CREATE OR REPLACE EXTERNAL TABLE 'taxi-rides-ny.nytaxi.external_yellow_tripdata'
OPTIONS (
    format = 'CSV',
    uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);
```

The transfer method `gs://` is reserved for google cloud services. An external table is stored outside of BigQuery.

### Partitioning in Bigquery

Partitioning by (e.g., query date) can significantly speed up queries, as it effectively reduces the size of the table BQ processes.

- Create a non-partitioned table from an external table:
```sql
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.external_yellow_tripdata_non_partitioned AS
    SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```

- Create a partitioned table from the external table:
```sql
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.external_yellow_tripdata_partitioned
PARTITION BY DATE(tpep_pickup_datetime) AS
    SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```

For an example, a query that is separated by date will scan 1.6 GB on a non-partitioned table, while the partitioned table will scan ~106 MB, hugely improving performance and cost.

### Clustering in Bigquery

Can also "cluster" by tag:

- Create a partitioned and clustered table:
```sql
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.external_yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
    SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```

## 3.1.2 Partitioning and Clustering

### Partitioning

When creating a partition table, can select:
- Time unit column
    - Daily (Default)
    - Hourly
    - Monthly/Yearly
- Ingestion time
- Inter range
- Limit of partitions: 4000

### Clustering

- Columns you specify used to colocate
- Order of column is important
    - Order of columns determines sort order of data
- Clustering improves:
    - Filter queries
    - Aggergate queries
- Can specify up to four top-level, non-repeated columns

**Tables under 1 GB don't show significant improvement with partitioning/clustering**
May make more sense to not partition/cluster with these tables

### Partitioning vs Clustering

| Clustering | Partitioning |
| - | - |
| Cost benefit unknown | Cost benefit upfront |
| More granular than partitioning alone allows | Partition-level management |
| Queries commonly use filters or aggregation against multiple particular columns | Filter/aggergate on single column |
| The cardinality of the number of values in a column/group of columns is large | |

### Clustering over Partitioning
Partitioning results in:
- a small amount of data per partition
- a large number of partitions beyond the limits on partitioned tables
- your mutation operations modifying the majority of partitions in the table frequently
    - If you're writing data every hour which modifies all partitions, may be better to cluster

### Automatic reclustering
As data is added to a clustered table:
- Newly inserted data can be written to blocks that contain key ranges that overlap with the key ranges in previously written blocks
- These overlapping keys weaken the sort property of the table

To maintain the performance of a clustered table:
- BigQuery performs automatic re-clustering in the background to restore the sort property of the table
- For partitioned tables, clustering maintained for data within the scope of each partition

## BigQuery Best Practices

- Cost reduction
    - Avoid `SELECT *`
    - Price queries before running them
    - Use clustered/partitioned tables
    - Use streaming inserts with caution
    - Materialize query results in stages