from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object, col
from pyspark.sql.types import IntegerType


def for_each_batch_func(df: DataFrame, epoch):
    print(f'epoch: {epoch} start')
    print(f'streaming dataframe show()')
    df.show(truncate=False)

    json_to_col_df = df.select(
        get_json_object('VALUE.name').alias('NAME'),
        get_json_object('VALUE.address.country').alias('COUNTRY'),
        get_json_object('VALUE.address.city').alias('CITY'),
        get_json_object('VALUE.age').cast(IntegerType()).alias('AGE')
    )
    json_to_col_df.persist()
    df_cnt = json_to_col_df.count()
    json_to_col_df.writeStream \
        .toTable('person_info', outputMode='append')

    print(f'json_to_col_df 저장 완료 ({df_cnt}건)')
    print(f'epoch: {epoch} end')


app_name = 'get_json_object'
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
        .foreachBatch(for_each_batch_func) \
        .option("checkpointLocation", f'/home/spark/kafka_offsets/{app_name}') \
        .start()

query.awaitTermination()