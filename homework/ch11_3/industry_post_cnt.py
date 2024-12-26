from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName('industry_post_cnt') \
        .config('spark.executor.instances','3') \
        .config('spark.executor.cores','1') \
        .getOrCreate()

postings_path = 'hdfs:///home/spark/lesson/linkedin_jobs/postings.csv'
postings_schema = ('job_id LONG,'
                   'company_name               STRING,'
                   ' title                     STRING,'
                   ' description               STRING,'
                   ' max_salary                LONG, '
                   'pay_period                 STRING, '
                   'location                   STRING, '
                   'company_id                 LONG, '
                   'views                      LONG, '
                   'med_salary                 LONG, '
                   'min_salary                 LONG, '
                   'formatted_work_type        STRING, '
                   'applies                    LONG, '
                   'original_listed_time       TIMESTAMP, '
                   'remote_allowed             STRING, '
                   'job_posting_url            STRING, '
                   'application_url            STRING, '
                   'application_type           STRING, '
                   'expiry                     STRING, '
                   'closed_time                TIMESTAMP, '
                   'formatted_experience_level STRING, '
                   'skills_desc                STRING, '
                   'listed_time                TIMESTAMP, '
                   'posting_domain             STRING, '
                   'sponsored                  LONG, '
                   'work_type                  STRING, '
                   'currency                   STRING, '
                   'compensation_type          STRING')
company_emp_path = 'hdfs:///home/spark/lesson/linkedin_jobs/companies/employee_counts.csv'
company_emp_schema = 'company_id     LONG,' \
                     'employee_count STRING,' \
                     'follower_count STRING,' \
                     'time_recorded  TIMESTAMP'
company_ind_path = 'hdfs:///home/spark/lesson/linkedin_jobs/companies/company_industries.csv'
company_ind_schema = 'company_id LONG, industry STRING'
rslt_path = 'hdfs:///home/spark/homework/ch11.3/industry_post_cnt'

## Program start
postings_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(postings_schema) \
                 .csv(postings_path)
postings_df.createOrReplaceTempView('postings')
print('postings_df load 완료')

company_emp_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(company_emp_schema) \
                 .csv(company_emp_path)
company_emp_df.createOrReplaceTempView('com_emp')
print('company_emp_df load 완료')

company_idu_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(company_ind_schema) \
                 .csv(company_ind_path)
company_idu_df.createOrReplaceTempView('com_idu')
print('company_idu_df load 완료')

sql = f'''
SELECT /*+ BROADCAST(e), BROADCAST(i) */
    i.industry, count(p.job_id) AS post_cnt
FROM postings p
JOIN com_emp e
    on p.company_id = e.company_id
JOIN com_idu i
    on p.company_id = i.company_id
WHERE 1=1
    AND p.job_id is not null
    AND e.employee_count >= 10000
GROUP BY i.industry
'''
rslt_df = spark.sql(sql).coalesce(3)
rslt_df.write \
        .format('parquet') \
        .save(rslt_path)

print('rslt_df 저장 완료')