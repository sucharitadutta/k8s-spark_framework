"""
Driver-side script for job_type = sql  (baked in the image: /opt/app/jobs/sql_runner.py)
  1. read source via the Scala jar  -> temp view
  2. run the SQL text sent by the wrapper
  3. write result via the Scala jar
"""
import argparse
import base64
import json

from pyspark.sql import DataFrame, SparkSession

READERS = {"TERADATA": "teradataRead", "ORACLE": "oracleRead", "SQLSERVER": "sqlServerRead",
           "MINIO": "minioRead", "DATALAKE": "datalakeRead"}
WRITERS = {"TERADATA": "teradataWrite", "ORACLE": "oracleWrite", "SQLSERVER": "sqlServerWrite",
           "MINIO": "minioWrite", "DATALAKE": "datalakeWrite"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    cfg = json.loads(base64.b64decode(ap.parse_args().config))
    src, tgt = cfg["source"], cfg["target"]

    spark = SparkSession.builder.appName(f"fw-{cfg['section']}").getOrCreate()
    sources = spark._jvm.com.acme.fw.SourceConnections
    targets = spark._jvm.com.acme.fw.TargetConnections

    # 1. read
    src_location = src.get("table") or src.get("path")
    jdf = getattr(sources, READERS[cfg["source_type"]])(spark._jsparkSession, src_location)
    DataFrame(jdf, spark).createOrReplaceTempView(src.get("view", "src"))

    # 2. transform
    result = spark.sql(cfg["sql"])

    # 3. write
    tgt_location = tgt.get("table") or tgt.get("path")
    getattr(targets, WRITERS[cfg["target_type"]])(result._jdf, tgt_location)

    print(f"[sql_runner] {cfg['section']} run_id={cfg['run_id']} SUCCESS")
    spark.stop()


if __name__ == "__main__":
    main()
