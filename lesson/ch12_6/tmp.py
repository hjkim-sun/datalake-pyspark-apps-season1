from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object
from pyspark.sql.types import IntegerType


app_name = 'sink_to_s3'
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

kafka_source_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
                .option("subscribe", "lesson.spark-streaming.person_info") \
                .option('startingOffsets','earliest') \
                .load() \
                .selectExpr(
                    "CAST(key AS STRING) AS KEY",
                    "CAST(value AS STRING) AS VALUE"
                ).select(
                    get_json_object('VALUE', '$.name').alias('NAME'),
                    get_json_object('VALUE', '$.address.country').alias('COUNTRY'),
                    get_json_object('VALUE', '$.address.city').alias('CITY'),
                    get_json_object('VALUE', '$.age').cast(IntegerType()).alias('AGE')
                )

spark.sql('CREATE DATABASE IF NOT EXISTS lesson')
spark.sql('USE lesson')
rslt = spark.sql(f'''CREATE TABLE IF NOT EXISTS person_info(
                name      STRING,
                country   STRING,
                city      STRING,
                age       INT)
              USING hive OPTIONS(fileFormat 'parquet')  
              LOCATION 's3a://datalake-spark-sink/lesson/person_info'
              '''
                 )
print('Table Create (if not exists) completed ')

query = kafka_source_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", f'hdfs:///home/spark/kafka_offsets{app_name}') \
        .toTable('person_info', outputMode='append')

query.awaitTermination()