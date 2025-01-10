from pyspark.sql.functions import col
path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/companies.csv'
schema = 'company_id STRING,name STRING,description STRING,company_size INT, state STRING, country STRING, city STRING, zip_code STRING, address STRING, url STRING'

csv_df = spark \
        .read \
        .option("header", 'true') \
        .option('multiLine','true') \
        .schema(schema) \
        .csv(path)
us_df = csv_df \
         .filter(col('country') == 'US')
us_df.persist()
us_df.count()
ny_df = us_df.filter(col('state') == 'NY')
ny_df.count()
us_df.unpersist()
