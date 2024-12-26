from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import get_json_object
from pyspark.sql.types import IntegerType

app_name = 'sink_to_s3'
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

df = spark.createDataFrame([('hjkim','korea','seoul',40),],'name STRING, country STRING, city STRING, age INT')

spark.sql('CREATE DATABASE IF NOT EXISTS lesson')
spark.sql('USE lesson')
rslt = spark.sql(f'''CREATE TABLE IF NOT EXISTS person_info(
                name      STRING,
                country   STRING,
                city      STRING,
                age       INT)
              LOCATION 's3a://datalake-spark-sink/lesson/person_info'
              STORED AS PARQUET
              '''
                 )
print(f'rslt: {rslt}')
print('Table Create (if not exists) completed ')

df.write \
        .format('hive') \
        .mode("append") \
        .saveAsTable("lesson.person_info")

