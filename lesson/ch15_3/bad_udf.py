from src.common.base_spark_job import BaseSparkJob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType
from datetime import datetime

class BadUdf(BaseSparkJob):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.app_name = app_name

    def _main(self):
        udf_get_birth = udf(BadUdf.get_birth2, IntegerType())
        spark = self.get_spark_session(SparkSession)
        df = spark.createDataFrame([('kim','seoul',30),('park','busan',21)],'NAME STRING, ADDRESS STRING, AGE INT')
        df = df.withColumn(
            'BIRTH_YEAR_STT_METHOD',
            BadUdf.get_birth('AGE')
        ).withColumn(
            'BIRTH_YEAR_DECORATOR',
            udf_get_birth('AGE')
        )
        df.show()

    # Decorator 적용 방식
    @staticmethod
    @udf(returnType=StringType())
    def get_birth(value):
        cur_year = datetime.now().year
        birth_year = cur_year - value
        return birth_year

    # 함수 적용 방식
    @staticmethod
    def get_birth2(value):
        cur_year = datetime.now().year
        birth_year = cur_year - value
        return birth_year
