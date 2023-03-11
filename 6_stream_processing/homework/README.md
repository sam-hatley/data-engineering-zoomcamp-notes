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