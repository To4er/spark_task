import os
import sys
from pyspark.sql import SparkSession
from config import PASSWORD, USER_NAME, JDBC_URL


def get_spark_session(app_name="MySparkJob"):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    hadoop_home = os.path.join(current_dir, "hadoop")
    os.environ['HADOOP_HOME'] = hadoop_home
    sys.path.append(os.path.join(hadoop_home, 'bin'))
    os.environ['PATH'] += os.pathsep + os.path.join(hadoop_home, 'bin')

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()

    return spark

db_properties = {
    "user": USER_NAME,
    "password": PASSWORD,
    "driver": "org.postgresql.Driver"
}


def full_dfs(spark: SparkSession, table_names: list) -> dict:
    dfs = {}
    for table in table_names:
        dfs[table] = spark.read.jdbc(
            url = JDBC_URL,
            table = f"public.{table}",
            properties = db_properties
        )
    return dfs
