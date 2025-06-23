from pyspark.sql import SparkSession

def get_spark_session(app_name):
    return (
        SparkSession.builder \
        .appName(app_name) \
        .master('local[*]') \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.submit.pyFiles", "/home/aduankan/Documents/Airflow/plugins/convert.zip,"
					"/home/aduankan/Documents/Airflow/plugins/spark_session.zip") \
    	.config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config('spark.dynamicAllocation.minExecutors', '1') \
        .config('spark.dynamicAllocation.maxExecutors', '2') \
        .config('spark.dynamicAllocation.enabled', 'true') \
        .config("spark.hadoop.fs.s3a.access.key", 'xdDPuIep2C9PzFaPQmJ7') \
        .config("spark.hadoop.fs.s3a.secret.key", 'GMqTLyTksNX75JrYUA1g2FfpePDJbQpqLJY6b4y2') \
        .config("spark.hadoop.fs.s3a.endpoint", "http://100.69.155.39:9000") \
        .config("spark.hadoop.fs.s3a.impl", 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config("spark.hadoop.fs.s3a.path.style.access", 'true') \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0," 
                "org.apache.hadoop:hadoop-aws:3.3.4," 
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", 'io.delta.sql.DeltaSparkSessionExtension') \
        .config("spark.sql.catalog.spark_catalog", 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .getOrCreate()
    )
