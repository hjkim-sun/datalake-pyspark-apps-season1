from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def for_each_batch_func(df: DataFrame, epoch_id, spark: SparkSession):
    df.createOrReplaceTempView('streaming_df')
    new_df = spark.sql('select * from streaming_df')


app_name = 'assign_spark_variable'
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

kafka_source_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
                .option("subscribe", "topic1") \
                .load() \
                .selectExpr(
                    "CAST(key AS STRING) AS KEY",
                    "CAST(value AS STRING) AS VALUE"
                )

query = kafka_source_df.writeStream \
        .foreachBatch(lambda df, epoch_id: for_each_batch_func(df, epoch_id, spark)) \
        .option("checkpointLocation", f'/home/spark/kafka_offsets/{app_name}') \
        .start()

query.awaitTermination()