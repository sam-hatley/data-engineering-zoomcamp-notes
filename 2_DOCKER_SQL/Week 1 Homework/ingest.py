import pandas as pd
from sqlalchemy import create_engine


engine = create_engine('postgresql://root:root@localhost:5432/ny_green_taxi')
schema = pd.io.sql.get_schema(df, name='green_taxi_data', con=engine)

df_iter = pd.read_csv('green_tripdata_2019-01.csv', iterator=True, chunksize=100_000)
df = next(df_iter)

df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')

df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
n = 100_000

while True:
    try:
        df = next(df_iter)
    except(StopIteration):
        break
    
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
    n += 100_000
    print(f'inserted chunk {n}')

df = pd.read_csv('taxi_zone_lookup.csv')
df.head(n=0).to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')
df.to_sql(name='taxi_zone_lookup', con=engine, if_exists='append')