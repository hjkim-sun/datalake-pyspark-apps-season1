from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName('load_postings') \
        .enableHiveSupport() \
        .getOrCreate()


postings_path = '/user/hive/warehouse/linkedIn'
postings_df = spark.read \
                    .option("path",postings_path) \
                    .table('postings')

postings_cnt = postings_df.count()
print(f'postings load 완료, count: {postings_cnt}')