# Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

```bash
docker build --help | grep ID
```

- `--iidfile string`


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an iterative mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

```bash
docker run -it --entrypoint=bash python:3.9
pip list
```

- 3

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

```sql
SELECT COUNT(*)
FROM green_taxi_data    
WHERE lpep_pickup_datetime >= '2019-01-15'
    AND lpep_dropoff_datetime < '2019-01-16';
```

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20530

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

```sql
SELECT lpep_pickup_datetime, trip_distance
FROM green_taxi_data
ORDER BY trip_distance DESC
LIMIT 10;
```

- 2019-01-15

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?

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

For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

Doing this the hard way.

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


## Submitting the solutions

* Form for submitting: TBA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Thursday), 22:00 CET


## Solution

We will publish the solution here
