package com.acme.fw

import org.apache.spark.sql.DataFrame

/*
 * TargetConnections.scala  --  WRITE side of the jar
 * Every method:  takes a DataFrame + "schema.table or path"  ->  writes it
 * Database targets APPEND (never drop a table). File targets OVERWRITE (safe to re-run).
 * User id / password / keys are fetched from Vault at run time.
 */
object TargetConnections {

  // ---------------- TERADATA ----------------
  def teradataWrite(df: DataFrame, table: String): Unit = {
    val host   = sys.env("TD_HOST")
    val secret = VaultClient.getSecret("pipelines/teradata")

    df.write
      .format("jdbc")
      .option("url", s"jdbc:teradata://$host/CHARSET=UTF8,TMODE=ANSI")
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .option("user", secret("username"))
      .option("password", secret("password"))
      .option("dbtable", table)
      .mode("append")
      .save()
  }

  // ---------------- ORACLE ----------------
  def oracleWrite(df: DataFrame, table: String): Unit = {
    val host    = sys.env("ORA_HOST")
    val service = sys.env("ORA_SERVICE")
    val secret  = VaultClient.getSecret("pipelines/oracle")

    df.write
      .format("jdbc")
      .option("url", s"jdbc:oracle:thin:@//$host:1521/$service")
      .option("driver", "oracle.jdbc.OracleDriver")
      .option("user", secret("username"))
      .option("password", secret("password"))
      .option("dbtable", table)
      .mode("append")
      .save()
  }

  // ---------------- SQL SERVER ----------------
  def sqlServerWrite(df: DataFrame, table: String): Unit = {
    val host     = sys.env("MSSQL_HOST")
    val database = sys.env("MSSQL_DB")
    val secret   = VaultClient.getSecret("pipelines/sqlserver")

    df.write
      .format("jdbc")
      .option("url", s"jdbc:sqlserver://$host:1433;databaseName=$database;encrypt=true;trustServerCertificate=true")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("user", secret("username"))
      .option("password", secret("password"))
      .option("dbtable", table)
      .mode("append")
      .save()
  }

  // ---------------- MINIO ----------------
  // path example: "bronze/orders"  ->  s3a://<bucket>/bronze/orders
  def minioWrite(df: DataFrame, path: String): Unit = {
    val endpoint = sys.env("MINIO_ENDPOINT")
    val bucket   = sys.env("MINIO_BUCKET")
    val secret   = VaultClient.getSecret("pipelines/minio")

    val hadoop = df.sparkSession.sparkContext.hadoopConfiguration
    hadoop.set("fs.s3a.endpoint", endpoint)
    hadoop.set("fs.s3a.access.key", secret("access_key"))
    hadoop.set("fs.s3a.secret.key", secret("secret_key"))
    hadoop.set("fs.s3a.path.style.access", "true")

    df.write.mode("overwrite").parquet(s"s3a://$bucket/$path")
  }

  // ---------------- DATALAKE ----------------
  // path example: "curated/orders"  ->  <DATALAKE_BASE_PATH>/curated/orders
  def datalakeWrite(df: DataFrame, path: String): Unit = {
    val basePath = sys.env("DATALAKE_BASE_PATH")   // e.g. hdfs://nn:8020/data

    df.write.mode("overwrite").parquet(s"$basePath/$path")
  }
}
