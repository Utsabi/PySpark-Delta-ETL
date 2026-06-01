from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from config.settings import SPARK_APP_NAME, SPARK_MASTER
 
 
def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """
    Create or retrieve a SparkSession with Delta Lake support.
    The delta-spark JAR is loaded via the 'packages' config.
    """
    conf = SparkConf()
    conf.set("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.sql.shuffle.partitions", "4")
 
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(app_name)
        .config(conf=conf)
        .getOrCreate()
    )
 
    spark.sparkContext.setLogLevel("WARN")
    return spark
 