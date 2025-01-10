from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from common.logger import Logger
import argparse

class BaseStreamApp():
    def __init__(self, app_name):
        self.app_name = app_name
        self.kafka_offset_dir = f'/home/spark/kafka_offsets/{app_name}'  # Kafka Offset Checkpoint 경로 지정

        # Logger 생성
        self.logger = Logger(app_name)

        # Spark Parameter 에 대한 설정
        # 잘 변경되지 않으며 고정되는 파라미터는 $SPARK_HOME/conf/spark-defaults.conf 에 설정하고
        # 프로그램마다 변경될 수 있는 파라미터들은 이 함수에 정의하도록 합니다.

        self.SPARK_DRIVER_CORES = '1'
        self.SPARK_DRIVER_MEMORY = '1g'
        self.SPARK_EXECUTOR_INSTANCES = '3'
        self.SPARK_EXECUTOR_MEMORY = '2g'
        self.SPARK_EXECUTOR_CORES = '2'
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '6'

        self.log_mode = ''
        self.set_argparse()

    def set_argparse(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-l", "--log_mode", default='info', help='info or debug')
        args = parser.parse_args()
        self.log_mode = args.log_mode

    def get_session_builder(self):
        return SparkSession \
            .builder \
            .appName(self.app_name) \
            .config('spark.driver.cores', self.SPARK_DRIVER_CORES) \
            .config('spark.driver.memory', self.SPARK_DRIVER_MEMORY) \
            .config('spark.executor.memory', self.SPARK_EXECUTOR_MEMORY) \
            .config('spark.executor.instances', self.SPARK_EXECUTOR_INSTANCES) \
            .config('spark.executor.cores', self.SPARK_EXECUTOR_CORES) \
            .config('spark.sql.shuffle.partitions', self.SPARK_SQL_SHUFFLE_PARTITIONS)

    def for_each_batch(self, df: DataFrame, epoch_id, spark: SparkSession):
        '''
        _for_each_batch 함수 실행 전, 후 공통 로직 삽입용도
        '''
        print(f'====================================== epoch_id: {epoch_id} start ======================================')
        self._for_each_batch(df, epoch_id, spark)
        print(f'====================================== epoch_id: {epoch_id} end ======================================')

    def _for_each_batch(self, df: DataFrame, epoch_id, spark: SparkSession):
        '''
        Spark Streaming Application 중 for each batch를 사용하는 경우 해당 함수를 재정의하여 사용합니다.
        '''
        pass
