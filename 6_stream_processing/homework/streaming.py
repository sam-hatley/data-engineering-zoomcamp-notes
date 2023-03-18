from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

RIDE_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("pickup_datetime", T.TimestampType()),
        T.StructField("pulocationid", T.IntegerType()),
        T.StructField("dolocationid", T.IntegerType()),
    ]
)


def read_from_kafka(consume_topic: str):
    df_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092")
        .option("subscribe", consume_topic)
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "checkpoint")
        .load()
    )

    return df_stream


def parse_ride_from_kafka_message(df, schema):
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df["value"], ", ")

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = "complete", processing_time: str = "5 seconds"):
    write_query = (
        df.writeStream.outputMode(output_mode)
        .trigger(processingTime=processing_time)
        .format("console")
        .option("truncate", False)
        .start()
    )
    return write_query  # pyspark.sql.streaming.StreamingQuery


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count().sort(F.desc("count"))
    return df_aggregation


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PULocations").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # read_streaming data
    df_consume_stream = read_from_kafka(consume_topic="PULocations")
    print(df_consume_stream.printSchema())

    # parse streaming data
    df_rides = parse_ride_from_kafka_message(df_consume_stream, RIDE_SCHEMA)
    print(df_rides.printSchema())

    sink_console(df_rides, output_mode="append")

    df_trip_count_by_id = op_groupby(df_rides, ["pulocationid"])

    # write the output out to the console for debugging / testing
    sink_console(df_trip_count_by_id)

    spark.streams.awaitAnyTermination()
