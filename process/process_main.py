import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from utils.spark_session import get_spark
from pyspark.sql import SparkSession
from process.silver_pipeline import run_silver_pipeline, read_new_bronze_data
from pyspark.sql.functions import current_timestamp, date_format
from utils.io_utils import read_last_crawl_time

if __name__ == '__main__':
    spark: SparkSession = get_spark()
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    last_crawl_time = read_last_crawl_time()
    job_run_date_str = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd-HH')").first()[0]

    run_silver_pipeline(spark, last_crawl_time, job_run_date_str)

    spark.sql("SHOW TABLES IN silver")