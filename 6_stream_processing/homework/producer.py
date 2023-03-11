# Planned to use pandas below, but I actually
# prefer the implementation with csv.

import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer


class RideCSVProducer:
    def __init__(self, properties: Dict):
        self.producer = KafkaProducer(**properties)

    @staticmethod
    def read_records(path: str, trip_type: str):
        records, ride_keys = [], []

        i = 0
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

                i += 1
                if i == 100:
                    break

        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for i in records:
            k, v = i
            self.producer.send(topic=topic, key=k, value=v)

        self.producer.flush()
        # I'm not sure why we're including the wait
        sleep(1)


if __name__ == "__main__":
    green_path = "../resources/green_fhv/green_tripdata_2019-01.csv"
    fhv_path = "../resources/green_fhv/fhv_tripdata_2019-01.csv"

    config = {
        "bootstrap_servers": ["localhost:9092"],
        "key_serializer": lambda x: x.encode("utf-8"),
        "value_serializer": lambda x: x.encode("utf-8"),
    }

    producer = RideCSVProducer(properties=config)

    # Grab FHV Records
    ride_records = producer.read_records(path=fhv_path, trip_type="FHV")
    print(ride_records)
    producer.publish(topic="PULocations", records=ride_records)

    # Grab Green Records
    ride_records = producer.read_records(path=green_path, trip_type="GREEN")
    print(ride_records)
    producer.publish(topic="PULocations", records=ride_records)
