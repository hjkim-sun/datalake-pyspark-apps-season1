from common.ch13_1.base_stream_app import BaseStreamApp
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType
from datetime import datetime
import pandas as pd
import time


class PandasUdf(BaseStreamApp):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.app_name = app_name

    def main(self):
        spark = self.get_session_builder() \
            .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
            .getOrCreate()

        df = spark.createDataFrame([('kim','seoul',30),('park','busan',21)],'NAME STRING, ADDRESS STRING, AGE INT')
        df = df.withColumn(
            'BIRTH_YEAR_STT_METHOD',
            PandasUdf.get_birth('AGE')
        )
        df.show()
        time.sleep(600)


    @staticmethod
    @pandas_udf(returnType=IntegerType())
    def get_birth(value: pd.Series) -> pd.Series:
        cur_year = datetime.now().year
        birth_year = cur_year - value
        return birth_year


if __name__ == '__main__':
    pandas_udf = PandasUdf(app_name='pandas_udf')
    pandas_udf.main()