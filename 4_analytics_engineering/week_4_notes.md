## 4.1.1 - Analytics Engineering Basics

### 1: What is Analytics Engineering?

#### Roles in a Data Team

| Data Engineer | Analytics Engineer | Data Analyst |
| - | - | - |
| Prepares and maintains infrastructure the data team needs | Introduces good software engineering practices to the efforts of data analysts and data scientists | Uses data to answer questions/solve problems |
| Better software engineers, less training in how to actually use the data | Tries to fill gap between Data Engs and Analysts | Not trained as software engineers, not first priority |
</br>

#### Tools of an Analytics Engineer

1. Data Loading
2. Data Storing
    - Cloud data warehouses like *Snowflake*, *BigQuery*, *Redshift*
3. Data Modelling
    - Tools like dbt or Dataform
4. Data Presentation
    - BI tools like Google data studio, Looker, Mode, or Tableau
</br>

### 2: Data Modelling Concepts

#### ETL vs ELT  

![ETL vs ELT](img/ELT_ETL.png)

| ETL | ELT |
| - | - |
| Takes longer to implement, first have to transform data | Faster and more flexible, takes advantage of cloud data warehousing |
| Slightly more stable and compliant data analysis | Faster/more flexible data analysis |
| Higher storage/compute costs | Lower cost/maintenance |</br>

#### Kimball's Dimensional Modeling

Objective:
- Deliver data understandable to the business users
- Deliver fast query performance

Approach:
- Prioritize user understandability and query performance over non-redundant data (3NF)

Other approaches:
- Bill Inmon
- Data Vault</br>

#### Elements of Dimensional Modeling

Also known as the *star schema*.

**Facts Tables**
- Measurements/metrics/facts
- Corresponds to a business process
- "*verbs*": sales, orders

**Dimensions Tables**
- Corresponds to a business *entity*
- Provides context to a business process
- "*nouns*": customer, product</br>

#### Architecture of Dimensional Modeling

Analogy: a restuant

**Stage Area**: *the ingredients*
- Contains the raw data
- Not meant to be exposed to everyone

**Processing Area**: *the kitchen*
- From raw data to data models
- Focuses in efficiency
- Ensuring standards

**Presentation Area**: *the dining area*
- Final presentation of the data
- Exposure to the business stakeholder</br>

 ### 3: What is dbt?

 [dbt: *Data Built Tool*](https://www.getdbt.com/)
 >A transformation too that allows anyone who knows SQL to deploy analytics code following software best practices like modularity, portability, CI/CD (Continuous Integration and Continuous Delivery), and documentation

 > dbt is a transformation workflow that helps you get more work done while producing higher quality results. You can use dbt to modularize and centralize your analytics code, while also providing your data team with guardrails typically found in software engineering workflows. Collaborate on data models, version them, and test and document your queries before safely deploying them to production, with monitoring and visibility.

#### How does dbt work?

Each model is:
- A *.sql file
- Select statement, no DDL or DML
- A file that dbt will compile/run in our data warehouse

#### How do you use dbt?

| dbt Core | dbt Cloud |
| - | - |
| The "essence" of dbt: compilation logic, macros, several db adapters | Web application to develop/manage dpt projects |
| Open-source project that allows data transformation |  SaaS app to develop and manage dbt projects |
| Builds/runs a dbt project (.sql/.yml) | Web-based IDE to develop, run, test a dbt project |
| Includes SQL compilation logic, macros, database adapters | Jobs orchestration |
| Includes a CLI interface to run dbt commands locally | Logging/alerting/integrated documentation |
| Open-source/free to use | Free for individuals |

#### How are we going to use dbt?

BigQuery
- Development using cloud IDE
- no local installation

Will take data from the source, import as raw data, and use dbt to transform data within the warehouse.

![DBT](img/DBT.png)

## 4.2.1 - Start Your dbt Project: BigQuery and dbt Cloud (Alternative A)

The taxi data are represented at the below links from 2019-01 - 2021-07:
https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

## 4.3.1 - Build the First dbt Models

### Anatomy of a dbt Model

```sql
{{ config(materialized='table') }}

SELECT *
FROM staging.source_table
WHERE record_state = 'ACTIVE'
```

The above compiles to:
```sql
CREATE TABLE my_schema.my_model AS(
    SELECT *
    FROM staging.source_table
    WHERE record_state = 'ACTIVE'
);
```

The curly brackets in the first expression are a *jinja* expression. At the beginning, usually use the *config* macro, in this case, a materialization strategy. There are several of these to choose from:
- Table: drops table if already exists in data warehouse, creates table in schema we're working with and with name of model
- View: the same, but ?
- Incremental: essentially a table, but allows us to build model incrementally
- Ephemeral: derived model

### The FROM clause of a dbt model



### Sources
- Data loaded to data warehouse used as a source
- Config defined in yml files in the models folder
- Used with the source macro that will resolve the name to the right schema, plus build dependencies automatically
- Source freshness can be defined/tested

```sql
FROM {{ source('staging', 'yellow_tripdata_2021_01') }}
WHERE vendorid IS NOT NULL
```

These defined in the below `yaml`:

```yaml
sources:
    - name: staging
      database: production
      schema: trips_data_all

      loaded_at_field: record_loaded_at
      tables:
        - name: green_tripdata
        - name: yellow_tripdata
          freshness:
            error_after: {count: 6, period: hour}
```

### Seeds
- CSV files in repo under seed folder
- Benefits of version control
- Equivalent to a copy command
- Rec'd for data that doesn't change frequently
- Runs with `dbt seed -s file_name`

```sql
SELECT
    locationid,
    borough,
    zone,
    REPLACE(service_zone, 'Boro' 'Green') AS service_zone
FROM {{ ref('taxi_zone_lookup') }}
```

**Ref**
- Macro to reference underlying tables/views in the data warehouse
- Run the same code in any envrionment, it will resove the correct schema for you
- Dependencies built automatically

dbt model:
```sql
WITH green_data AS (
    SELECT *,
           'Green' as Service_type
    FROM {{ ref('stg_green_tripdata') }}
);

Compiled code:
WITH green_data AS (
    SELECT *,
           'Green' as Service_type
    FROM "production"."dbt_victoria_mola"."stg_green_tripdata"
);
```

### Building the First Models

To build the first models:
1. created staging and core folders in models
2. created a sql file called `stg_green_tripdata.sql` in staging, with the below code
```sql
{{ config(materialized='view') }}

SELECT *
FROM trips_data_all.green_tripdata
```
3. Replaced the `FROM` statement with a source macro by creating a schema yaml file `schema.yaml`, using the code below. 'Database' refers to the first element in the table name in BigQuery, i.e., the dataset, `dezoomcamp-1` in this case. The schema is the second portion of that address.

```yaml
version: 2

sources:
    - name: staging
      database: dezoomcamp-1
      schema: trips_data_all

      tables:
        - name: green_tripdata
        - name: yellow_tripdata
```

Following this step, we could replace the `FROM` statement in step 2 with the below reference, specifying the table name:
```sql
FROM {{ source('staging', 'green_tripdata') }}
```

4. In the dbt console, we ran the model with a simple `dbt run` command: this runs every model, including the examples. (You can select models with `dbt run --select model_name`)

To do this, I had to first:
- Specify the location in dbt- it defaults to "US" and
- Create the dataset dbt_hatleys, as was specified during profile creation

I had to wait several minutes for the view to populate in BQ.

### Macros
- Use control structures (`if` statements & `for` loops) in SQL
- Use envrionment vars in your dbt project for production deployments
- Operate on the results of one query to generate another query
- Abstract snippets of SQL into reusable macros- analogous to functions in most programming languages

#### Definition of the macro
```sql
{#
    This macro returns the description of the payment_type
#}

{% macro get_payment_type_description(payment_type) -%}

    CASE {{ payment_type }}
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
    END

{%- endmacro %}
```

#### Usage of the macro
```sql
SELECT
    {{ get_payment_type_description('payment_type') }} AS payment_type_description,
    congestion_surcharge::DOUBLE PRECISION
FROM {{ source('staging', 'green_tripdata_2021_01') }}
WHERE vendorid IS NOT NULL
```

#### Compiled Macro
```sql
SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
    END AS payment_type_description,
    congestion_surcharge::DOUBLE PRECISION
FROM "production"."staging"."green_tripdata_2021_01"
WHERE vendorid IS NOT NULL
```

>At this point, I had to go back into my data and re-create all tables after correctly specifying dtype, as the default of float64 threw errors- annoying and time consuming. This was done by connecting a [yaml file](dtypes.yaml) containing the dtypes to our [cleaning function](load_to_bucket_parquet.py)

```py
df = df.astype(
    {
        "VendorID": "Int64",
        f"{dtstr}_pickup_datetime": "datetime64",
        f"{dtstr}_dropoff_datetime": "datetime64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "object",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }}
```

### Packages

-Like libraries in other programming languages
-Standalone dbt projects with models and macros that tackle a specific problem area
- Imported in the `packages.yml` file and imported by running `dbt deps`
- A list of useful packages can be found in [dbt package hub](https://hub.getdbt.com/)

An example `packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
```

To install these packages, run `dbt deps` from within the dbt console.

dbt_utils.surrogate_key can be inserted into our *.sql file using a jinja expression:
```sql
{{ config(materialied='view') }}

SELECT
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} AS tripid,
    CAST(vendorid AS INTEGER) AS vendorid,
    ...
```

### Variables
- Useful for defining values that should be used across the project
- To use, use the `{{ var('...') }}` function
- Can be defined in two ways:
    - dbt_project.yml file
    - on the command line

**Variable defined under project.yml:**
```yml
vars:
  payment_type_values: [1, 2, 3, 4, 5, 6]
```

**Variable that can be changed in command line:**
```sql
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}
```

### dbt Seeds

- CSV files that are kept in dbt repo
- meant for smaller files with info that does not change often (e.g., dimension tables)

You can do this locally, or by copy/paste: dbt won't let you upload files. Once that's done, add a "seeds" entry in dbt_project.yml with a reference to the seed, i.e.:

```yml
seeds:
  taxi_rides_ny:
    taxi_zone_lookup:
      -column _types:
        locationid: numeric
```

If you change entries in the table, you can refresh it with `dbt seed --full-refresh`.

## 4.3.2 - Testing and Documenting dbt Models

### Tests
- Assumptions we make about the data
- Tests essentially a `SELECT` query
- Assumptions compiled to sql that returns number of failing records
- Defined on a column in the .yml file
- dbt provides basic tests to check if a column value is:
    - unique
    - not null
    - an accepted value
    - a foreign key

**Basic tests:**
```yml
- name: payment_type_description
  description: Description of th epayment_type code
  tests:
    - accepted_values:
      values: [1, 2, 3, 4, 5]
      severity: warn
- name: Pickup_locationid
  description: locationid where the meter was engaged
  tests:
    - relationships:
      to: ref('taxi_zone_lookup')
      field: locationid
      severity: warn
- name: tripid
  description: Primary key
  tests:
    - unique:
      severity: warn
    - not_null:
      severity: warn
```

**Compiled code of the not_null test**
```sql
SELECT *
FROM "production"."dbt_name"."stg_yellow_tripdata"
WHERE tripid IS NULL
```

### Documentation

- dbt provides a way to generate documentation for your project and render it as a website
- Includes
    - Info about the project:
        - Model code
        - Dependencies
        - Sources
        - Auto Generated DAG
        - Descriptions from yml file and tests
    - Info about the DWH
        - Column names/data types
        - Table stats like size/rows

## 4.4.1 - Deployment Using dbt Cloud

### What is deployment?
- Running the models in dev in prod
- Workflow like:
    1. Develop in a user branch
    2. Open a pull request to merge into the main branch
    3. Merge
    4. Run new models in prod using the main branch
    5. Schedule the models

### Running a dbt project in prod
- dbt cloud includes a scheduler
- A single job can run multiple commands
- Jobs can be triggered manually or on schedule
- Each job will keep a log of runs over time
- Each run will have the logs for each command
- Jobs can also generate documentation that can be viewed under the run information
- If source freshness was run, results can be viewed at the end of a job

### What is Continuous Integration (CI)?
- Practice of regularly merging dev branches into a central repository, after which automated builds/tests are run
- Goal is to reduce adding bugs to prod and mantain a (more) stable project
- dbt allows us to enable CI on pull requests enabled via webhooks via GitHub or GitLab
- When webhook is received, enqueues a new job against a temp schema: the pull request cannot be merged if the run fails
> The first commit can be committed to main, subsequent commits must involve a pull request

This can be setup pretty easily:
1. in dbt cloud, go to deploy > environments and set up a prod env using the prod dataset
2. go to deploy > jobs and create a job- in this case, one that launches `dbt build`
3. you can enable docs, enforce source freshness
4. in this case, we're running the following commands in this order:
    - `dbt seed`, which ensures that seed csvs are up-to-date
    - `dbt run`, which runs our models
    - `dbt test`, which tests that our models have run properly
5. Set a schedule, if warrented, and in the CI tab run on pull requests.


## 4.5.1 - Visualising the data with Google Data Studio

1. Go to [Google Data Studio](datastudio.google.com)
2. Create a Data Source: in this case, production -> fact_trips
3. Customize the default aggregation method: here, it's choosing to sum dropoff_locationid and pickup_locationid, which is nonsensical. Can also set descriptions, etc. Create the report.
4. Run over to looker to build the information we need.