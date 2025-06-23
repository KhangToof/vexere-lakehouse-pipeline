from pyspark.sql import SparkSession
from spark_session.spark_config import get_spark_session
from datetime import datetime

def convert(file_type="ticket"):
    spark = get_spark_session("convert")

    _date = datetime.now().strftime("%d-%m-%Y")
    _month_year = datetime.now().strftime("%m-%Y")
    
    if file_type == "ticket":
        file_path = f"file:///home/aduankan/Documents/Airflow/raw/ticket/{_month_year}/bus_data_{_date}.csv"
        s3_path = f"s3a://bronze/ticket/{_month_year}/{_date}/"
        raw = spark.read.csv(file_path, header=True)
        raw.write.format("delta").mode("overwrite").save(s3_path)

    elif file_type == "facility":
        file_path = f"file:///home/aduankan/Documents/Airflow/raw/facility/bus_facilities.json"
        s3_path = "s3a://bronze/facility/"
        raw = spark.read.json(file_path)
        raw.write.format("delta").mode("overwrite").save(s3_path)

    elif file_type == "review":
        file_path = f"file:///home/aduankan/Documents/Airflow/raw/review/bus_reviews.json"
        s3_path = f"s3a://bronze/review/"
        raw = spark.read.json(file_path)
        raw.write.format("delta").mode("overwrite").save(s3_path)

    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    spark.stop()
