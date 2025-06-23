from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from spark_session.spark_config import get_spark_session

def write_audit_logs(**kwargs):
    spark = get_spark_session("audit_writer")
    audit_path = "s3a://audit/audit"

    audit_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("dag_id", StringType(), True),
        StructField("task_id", StringType(), True),
        StructField("state", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("try_number", IntegerType(), True),
        StructField("hostname", StringType(), True)
    ])

    # Check if Delta table exists
    if not spark._jsparkSession.catalog().tableExists(f"delta.`{audit_path}`"):
        spark.createDataFrame([], audit_schema).write.format("delta").mode("overwrite").save(audit_path)

    dag = kwargs['dag']
    ti = kwargs['ti']
    current_task_id = ti.task_id

    logs = []
    for task in dag.tasks:
        if task.task_id == current_task_id:
            continue  # Bỏ qua task hiện tại

        task_instance = ti.get_dagrun().get_task_instance(task.task_id)
        if task_instance and task_instance.state:
            logs.append({
                "timestamp": datetime.utcnow().isoformat(),
                "dag_id": dag.dag_id,
                "task_id": task.task_id,
                "state": task_instance.state,
                "start_time": task_instance.start_date.isoformat() if task_instance.start_date else None,
                "end_time": task_instance.end_date.isoformat() if task_instance.end_date else None,
                "duration_seconds": task_instance.duration if task_instance.duration else None,
                "try_number": task_instance.try_number,
                "hostname": task_instance.hostname
            })

    if logs:
        df = spark.createDataFrame(pd.DataFrame(logs), audit_schema)
        df.write.format("delta").mode("append").save(audit_path)
