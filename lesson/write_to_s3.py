from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName('simple_pyspark') \
        .config('spark.hadoop.fs.s3a.access.key','##spark_aws_access_key##') \
        .config('spark.hadoop.fs.s3a.secret.key','##spark_aws_access_secret_key##') \
        .getOrCreate()

customer_df = spark.read \
               .parquet('hdfs:///home/spark/lesson/parquet/customers.parq')

print('customers.parq read from HDFS 완료')
s3_bucket_nm = 'datalake-actions-deploy'
s3_file_nm = 'pyspark/lesson/parquet/customers.parq'

customer_df.write \
            .parquet(f's3a://{s3_bucket_nm}/{s3_file_nm}')
