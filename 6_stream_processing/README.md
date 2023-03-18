# Week 6: Stream Processing

## 6.3-What is kafka?

Producers create material for a 'topic': a stream of events, which is passed to consumers. Logs are events within a topic. Messages are within logs (?). Messages have a key, a value, and a timestamp.

Compared to other solutions, Kafka provides reliability, somehow. It also adds flexibility, somehow. Also scalability, somehow. 

## 6.4-Confluent cloud

We're using [confluent cloud](https://www.confluent.io/en-gb/confluent-cloud/). Woah. It's *"Fully Managed Kafka as a Cloud-Native Service"*, so you don't need to understand anything about setup.

Tutorial steps
1. Create an account to use up those free credits
2. Create a basic cluster with GCP
3. Create an API key
4. Create a topic with two partitions and one day of retention, because because
5. Create a message because because
6. Create a Datagen Source connector with global access:
    - JSON output records
    - orders template
    - Smallest connector sizing (1)
7.  Shut it down
8. Wow, you did a thing! Pat yourself on the back!

## 6.5-Kafka producer consumer

This is where it gets interesting.

Using java- nope.

## 13 1 kafka stream python 1 (wut)

Stopped at 13:35


```
236 Ride: {'vendor_id': '1', 'tpep_pickup_datetime': (datetime.datetime(2020, 7, 1, 1, 55, 54),), 'tpep_dropoff_datetime': (datetime.datetime(2020, 7, 1, 2, 5, 23),), 'passenger_count': 1, 'trip_distance': Decimal('3.20'), 'rate_code_id': 1, 'store_and_fwd_flag': 'N', 'pu_location_id': 236, 'do_location_id': 230, 'payment_type': '2', 'fare_amount': Decimal('11.5'), 'extra': Decimal('3'), 'mta_tax': Decimal('0.5'), 'tip_amount': Decimal('0'), 'tolls_amount': Decimal('0'), 'improvement_surcharge': Decimal('0.3'), 'total_amount': Decimal('15.3'), 'congestion_surcharge': Decimal('2.5')}
```