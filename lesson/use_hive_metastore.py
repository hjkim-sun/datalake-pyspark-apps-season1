from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName('use_hive_metastore') \
        .enableHiveSupport() \
        .getOrCreate()

# 경로, 스키마 지정
postings_path = 'hdfs:///home/spark/lesson/linkedin_jobs/postings.csv'
postings_schema = ('job_id LONG, company_name STRING, title STRING, description STRING, max_salary LONG, pay_period '
                   'STRING, location STRING, company_id LONG, views LONG, med_salary LONG, min_salary LONG, '
                   'formatted_work_type STRING, applies LONG, original_listed_time TIMESTAMP, remote_allowed STRING, '
                   'job_posting_url STRING, application_url STRING, application_type STRING, expiry STRING, '
                   'closed_time TIMESTAMP, formatted_experience_level STRING, skills_desc STRING, listed_time '
                   'TIMESTAMP, posting_domain STRING, sponsored LONG, work_type STRING, currency STRING, '
                   'compensation_type STRING')

# dataframe load
postings_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(postings_schema) \
                 .csv(postings_path)

# dataframe saveAsTable
postings_df.write \
                .saveAsTable('linkedIn/postings')