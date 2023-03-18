The datasets required were cleaned a bit before processing: namely, I wanted to drop all *na* vals for PUlocationID in the FHV dataset, and strip out the whitespace around the distpatching base numbers. This cost a couple million rows, but works for the sake of making something simple.

```py
fhv_loc = '../resources/green_fhv/fhv_tripdata_2019-01.csv'
df = pd.read_csv(fhv_loc,
                 engine= 'pyarrow')
df = df[df['PUlocationID'].notna()]
df['dispatching_base_num'] = df['dispatching_base_num'].str.strip()
df = df[df['dispatching_base_num'] != '\\N']
df = df[df['PUlocationID'].notna()]
df['dispatching_base_num'] = df['dispatching_base_num'].str.strip()
df = df[df['dispatching_base_num'] != '\\N']
df['dispatching_base_num'] = df['dispatching_base_num'].str.replace('B', '')
df['dispatching_base_num'] = df['dispatching_base_num'].str.replace('b', '')
df['dispatching_base_num'] = df['dispatching_base_num'].astype('Int32')
df.to_csv(fhv_loc,
          index=False)
```

Rather than electing to produce two separate streams that would later be merged, I've sent both FHV and green taxi data out in the same stream from the [producer script](producer.py), by choosing comparable fields with the characteristics we're looking for. Within the producer script, the settings look like the below, which is re-tooled from the example.

```py
with open(path, "r") as f:
    reader = csv.reader(f)
    header = next(reader)  # skip the header
    for row in reader:
        if trip_type.upper() == "GREEN":
            # VendorID, lpep_pickup_datetime, PULocationID, DOLocationID
            records_str = f"{row[0]}, {row[1]}, {row[5]}, {row[6]}"
        elif trip_type.upper() == "FHV":
            # dispatching_base_num, pickup_datetime, PULocationID, DOLocationID
            records_str = f"{row[0]}, {row[1]}, {row[3]}, {row[4]}"
        else:
            raise ValueError()

        records.append(records_str)
        ride_keys.append(str(row[0]))
...
```

The [streaming program](streaming.py) reworks the `op_groupby()` function as written below to group POlocationIDs and order by usgage:

```py
def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count().sort(F.desc("count"))
    return df_aggregation
```
Which ultimately produces a table like the below:
```
-------------------------------------------
Batch: 1
-------------------------------------------
+------------+-----+
|pulocationid|count|
+------------+-----+
|265         |125  |
|181         |40   |
|42          |40   |
|25          |35   |
|145         |30   |
|41          |25   |
|173         |25   |
|7           |25   |
|237         |25   |
|70          |25   |
|255         |20   |
|236         |20   |
|223         |20   |
|97          |20   |
|256         |20   |
|56          |20   |
|66          |20   |
|146         |15   |
|49          |15   |
|129         |15   |
+------------+-----+
only showing top 20 rows
```
