from pyspark.sql import SparkSession
import os

def get_spark_session(app_name: str, local: bool = True) -> SparkSession:
    """
    Creates or gets a Spark Session configured for Delta Lake.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")

    if local:
        # Optimization for local mode
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "2g")
            
    spark = builder.getOrCreate()
    
    # Adjust log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark