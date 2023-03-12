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

Frankly, I spent about two days attempting to debug the error below: there is nothing on google, and I'm afraid that it may have had something to do with the way I initially setup spark on both my local and remote machines: I considered reinstalling the program from scratch to debug, but as I do not intend to use this particular aspect of the course in the near future, I believed that this would best be left for another time when I'm able to learn the ins and outs of faust. To be clear, this was tested with my files and the original files on both a local and remote machine. This error only occurs when running streaming.py with the associated bash script.

```
23/03/12 14:48:38 ERROR TransportRequestHandler: Error while invoking RpcHandler#receive() on RPC id 8890274778608944880
java.io.InvalidClassException: org.apache.spark.storage.BlockManagerMessages$RegisterBlockManager; local class incompatible: stream classdesc serialVersionUID = -5464070606352341135, local class serialVersionUID = -699032251763294541
```

As time constraints were high, I elected to create a stream setup that follows the letter of the exercise, rather than the spirit- the producer script is designed to be used as-is as a python script, while the consumer in this case, is a jupyter notebook.


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

The [consumer notebook](streaming_notebook.ipynb) utilises the memory sink as written below to group POlocationIDs and order by usgage:

```py
def sink_memory(df, query_name, query_template):
    write_query = df \
        .writeStream \
        .queryName(query_name) \
        .format('memory') \
        .start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return write_query, query_results

query_name = 'pulocationid_counts'
query_template = 'select count(distinct(pulocationid)) from {table_name}'
write_query, df_pulocationid_counts = sink_memory(df=df_rides, query_name=query_name, query_template=query_template)

query_name = 'pulocationid_counts'
query_template = '''
    SELECT DISTINCT(pulocationid) AS PUlocation,
        COUNT(*) AS trips_taken
    FROM {table_name}
    GROUP BY PUlocation
    ORDER BY trips_taken DESC
'''

write_query, df_pulocationid_counts = sink_memory(df=df_rides, query_name=query_name, query_template=query_template)

df_pulocationid_counts.show()
```
Which ultimately produces a table like the below:
```
+----------+-----------+
|PUlocation|trips_taken|
+----------+-----------+
|       265|         25|
|        42|          8|
|       181|          8|
|        25|          7|
|       145|          6|
|        70|          5|
|        41|          5|
|       173|          5|
|       237|          5|
|         7|          5|
|        56|          4|
|       236|          4|
|       255|          4|
|       223|          4|
|       256|          4|
|        97|          4|
|        66|          4|
|       146|          3|
|        95|          3|
|        49|          3|
+----------+-----------+
only showing top 20 rows
```

As said above, this implementation follows the letter of the exercise, rather than its spirit. This is a subject with conserable depth, and I will certainly take the time to learn more of in due course.