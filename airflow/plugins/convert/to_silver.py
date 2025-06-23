from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    regexp_replace, split, col, size, array_contains, lower, lit,
    to_date, when, max as spark_max, round as F_round,
    row_number, monotonically_increasing_id, explode, udf
)
from pyspark.sql.types import FloatType, IntegerType, ArrayType, StringType
from pyspark.sql.window import Window
from datetime import datetime
from typing import Optional
import traceback
import re
from spark_session.spark_config import get_spark_session

def standardize_string(text: str) -> Optional[str]:
    if text is None:
        return None
    try:
        text = text.lower()

        replacements = [
            (r"\b(q([0-9]+))\b", r"quận \2"),
            (r"(?<=\w)(ql\s*([0-9]+[a-z]?))\b", r" quốc lộ \2"),
            (r"\b(ql\s*([0-9]+[a-z]?))\b", r"quốc lộ \2"),
            (r"\btp\b", "thành phố"),
            (r"\bvp\b", "văn phòng"),
            (r"\bkcn\b", "khu công nghiệp"),
            (r"\bhcm\b", "hồ chí minh"),
            (r"\bbx\b", "bến xe"),
            (r"\btx\b", "thị xã"),
            (r"\bsg\b", "sài gòn"),
            (r"\bubnd\b", "uỷ ban nhân dân"),
            (r"\btt\b", ""),
            (r"\bcd\b", "")
        ]

        for pattern, repl in replacements:
            text = re.sub(pattern, repl, text)

        specific_replacements = [
            (r"\bsai gon nga tư ga\b", "sài gòn ngã 4 ga"),
            (r"\bsai gon\b", "sài gòn"),
            (r"\bngã tư an sươngquốc lộ 1a\b", "ngã tư an sương quốc lộ 1a"),
            (r"sg sài gòn", "sài gòn"),
            (r"sài gòn sài gòn", "sài gòn")
        ]

        for pattern, repl in specific_replacements:
            text = re.sub(pattern, repl, text)

        vietnamese_chars = "áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ"
        text = re.sub(fr"[^a-z0-9\s{vietnamese_chars}]", "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s+', ' ', text).strip()

        return text
    except Exception as e:
        print(f"Error standardizing text: '{text[:50]}...' - {e}")
        return None

def convert_duration(duration):
    if duration is None:
        return None
    match = re.match(r"(\d+)h?(\d*)m?", duration)
    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        return hours + minutes / 60
    return None

def remove_price_chars(price_str):
    if price_str:
        cleaned = re.sub(r"[^0-9]", "", price_str)
        return int(cleaned) if cleaned else None
    return None

def add_bus_id(bus_ticket, bus_ids_company, join_column="Bus_Name"):
    df = bus_ticket.join(bus_ids_company.select(col(join_column), col("Bus_Id")), on=join_column, how="left")
    ordered = ["Bus_Key", "Bus_Id", "Bus_Name"] + [c for c in df.columns if c not in ["Bus_Key", "Bus_Id", "Bus_Name"]]
    return df.select(*ordered)

def add_bus_id_z(bus_facility, bus_ids_company, join_column="Bus_Name"):
    df = bus_facility.join(bus_ids_company.select(col(join_column), col("Bus_Id")), on=join_column, how="left")
    ordered = ["Id", "Bus_Id", "Bus_Name"] + [c for c in df.columns if c not in ["Id", "Bus_Id", "Bus_Name"]]
    return df.select(*ordered)

def add_bus_id_x(bus_review, bus_ids_company, join_column="Bus_Name"):
    df = bus_review.join(bus_ids_company.select(col(join_column), col("Bus_Id")), on=join_column, how="left")
    ordered = ["Review_Key", "Bus_Id", "Bus_Name"] + [c for c in df.columns if c not in ["Review_Key", "Bus_Id", "Bus_Name"]]
    return df.select(*ordered)

def ticket_to_silver():
    spark = get_spark_session("ticket_to_silver")

    try:
        _date = datetime.now().strftime("%d-%m-%Y")
        _month_year = datetime.now().strftime("%m-%Y")
        source_path = f"s3a://bronze/ticket/{_month_year}/{_date}"
        target_path = "s3a://silver/ticket/"
        
        new_data = spark.read.format("delta").load(source_path)
        bus_ids = spark.read.format("delta").load("s3a://silver/bus_ids/")
        
        try:
            existing = spark.read.format("delta").load(target_path)
            max_id = existing.select(spark_max(col("Bus_Key").cast(IntegerType()))).first()[0] or 0
        except:
            max_id = 0

        new_data = new_data.withColumn("Start_Date", to_date("Start_Date", "dd-MM-yyyy"))
        new_data = new_data.withColumn("Departure_Place", udf(standardize_string, StringType())("Departure_Place"))
        new_data = new_data.withColumn("Arrival_Place", udf(standardize_string, StringType())("Arrival_Place"))
        new_data = new_data.withColumn("Duration", F_round(udf(convert_duration, FloatType())("Duration"), 2))
        new_data = new_data.withColumn("Price", udf(remove_price_chars, IntegerType())("Price"))
        new_data = new_data.withColumn("Type_Bus", lower(col("Type_Bus")))

        new_data = new_data.withColumn(
            "Bus_Type_Category",
            when(col("Type_Bus").like("%giường nằm%") & ~col("Type_Bus").like("%limousine%"), "giường nằm")
            .when(col("Type_Bus").like("%huyndai solati 11 chỗ%"), "limousine ghế ngồi")
            .when(col("Type_Bus").like("%limousine%") & col("Type_Bus").like("%ghế ngồi%"), "limousine ghế ngồi")
            .when(col("Type_Bus").like("%limousine%") & col("Type_Bus").like("%giường nằm có wc%"), "limousine giường nằm có WC")
            .when(col("Type_Bus").like("%limousine%") & (col("Type_Bus").like("%giường nằm%") | col("Type_Bus").like("%giường%")), "limousine giường nằm")
            .when(col("Type_Bus").rlike(r"limousine.*[0-9]+\s*chỗ"), "limousine giường nằm")
            .when(col("Type_Bus").like("%phòng%"), "limousine giường phòng")
            .when(col("Type_Bus").like("%ghế ngồi%"), "ghế ngồi")
            .otherwise("khác")
        )

        new_data = new_data.withColumn("temp_id", monotonically_increasing_id())
        new_data = new_data.withColumn("Bus_Key", row_number().over(Window.orderBy("temp_id")) + lit(max_id))
        new_data = new_data.drop("temp_id").withColumn("Bus_Key", col("Bus_Key").cast(IntegerType()))

        final_data = add_bus_id(new_data, bus_ids)
        final_data.write.format("delta").mode("append").save(target_path)

    except Exception:
        traceback.print_exc()
    finally:
        spark.stop()

def facility_to_silver():
    spark = get_spark_session("facility_to_silver")

    try:
        facility_df = spark.read.format("delta").load("s3a://bronze/facility/")
        if isinstance(facility_df.schema["Facilities"].dataType, ArrayType):
            facility_df = facility_df.filter((size(col("Facilities")) > 0) & (~array_contains(col("Facilities"), "")))
        else:
            facility_df = facility_df.withColumn("Facilities", regexp_replace(col("Facilities"), r"[\\[\\]']", ""))
            facility_df = facility_df.withColumn("Facilities", split(col("Facilities"), ", "))
            facility_df = facility_df.filter((size(col("Facilities")) > 0) & (~array_contains(col("Facilities"), "")))

        bus_ids = spark.read.format("delta").load("s3a://silver/bus_ids/")
        facility_df = add_bus_id_z(facility_df, bus_ids)

        facility_names = facility_df.select(explode("Facilities").alias("Facility_Name")).distinct()
        facility_names = facility_names.withColumn("Facility_Id", row_number().over(Window.orderBy("Facility_Name")))

        exploded = facility_df.select("Bus_Id", "Bus_Name", explode("Facilities").alias("Facility_Name"))
        bus_facility_id = exploded.join(facility_names, on="Facility_Name").select("Bus_Id", "Bus_Name", "Facility_Id").distinct()

        bus_facility_id.write.format("delta").mode("append").save("s3a://silver/facility/")
        facility_names.write.format("delta").mode("append").save("s3a://silver/facility_name/")

    except Exception:
        traceback.print_exc()
    finally:
        spark.stop()

def review_to_silver():
    spark = get_spark_session("review_to_silver")
    json_path = 'file:///home/aduankan/Documents/Airflow/raw/sentiment/sentiment_vi_results_temp.json'
    json_patha = 'file:///home/aduankan/Documents/Airflow/raw/sentiment/sentiment_en_results_temp.json'

    try:
        review_df_vi = spark.read.option("multiLine", True).json(json_path)
        review_df_en = spark.read.option("multiLine", True).json(json_patha)
        bus_ids = spark.read.format("delta").load("s3a://silver/bus_ids/")

        try:
            existing = spark.read.format("delta").load("s3a://silver/bus_reviews_vi/")
            max_id = existing.select(spark_max(col("Review_Key").cast(IntegerType()))).first()[0] or 0
        except:
            max_id = 0

        review_df_vi = review_df_vi.withColumn("temp_id", monotonically_increasing_id())
        review_df_vi = review_df_vi.withColumn("Review_Key", row_number().over(Window.orderBy("temp_id")) + lit(max_id))
        review_df_vi = review_df_vi.drop("temp_id").withColumn("Review_Key", col("Review_Key").cast(IntegerType()))

        df_vi = add_bus_id_x(review_df_vi, bus_ids)
        df_vi.write.format("delta").mode("append").save("s3a://silver/bus_reviews_vi/")

        try:
            existing = spark.read.format("delta").load("s3a://silver/bus_reviews_en/")
            max_id = existing.select(spark_max(col("Review_Key").cast(IntegerType()))).first()[0] or 0
        except:
            max_id = 0

        review_df_en = review_df_en.withColumn("temp_id", monotonically_increasing_id())
        review_df_en = review_df_en.withColumn("Review_Key", row_number().over(Window.orderBy("temp_id")) + lit(max_id))
        review_df_en = review_df_en.drop("temp_id").withColumn("Review_Key", col("Review_Key").cast(IntegerType()))

        df_en = add_bus_id_x(review_df_en, bus_ids)
        df_en.write.format("delta").mode("append").save("s3a://silver/bus_reviews_en/")

    except Exception:
        traceback.print_exc()
    finally:
        spark.stop()
