from common.ch13_1.base_stream_app import BaseStreamApp
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from datetime import datetime
import time


class BadUdf(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.app_name = app_name

    def main(self):
        spark = self.get_session_builder().getOrCreate()
        udf_get_birth = udf(BadUdf.get_birth2, IntegerType())
        df = spark.createDataFrame([('kim','seoul',30),('park','busan',21)],'NAME STRING, ADDRESS STRING, AGE INT')
        df = df.withColumn(
            'BIRTH_YEAR_STT_METHOD',
            BadUdf.get_birth('AGE')
        ).withColumn(
            'BIRTH_YEAR_DECORATOR',
            udf_get_birth('AGE')
        )
        df.show()
        time.sleep(600)

    # Decorator 적용 방식
    @staticmethod
    @udf(returnType=IntegerType())
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


if __name__ == '__main__':
    bad_udf = BadUdf(app_name='bad_udf')
    bad_udf.main()


