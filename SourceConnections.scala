package com.acme.fw

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * SourceConnections.scala  --  READ side of the jar
 * Every method:  takes spark + "schema.table"  ->  returns a DataFrame
 * Host / bucket (not secret)  : environment variables
 * User id / password / keys   : fetched from Vault at run time
 */
object SourceConnections {

  // ---------------- TERADATA ----------------
  def teradataRead(spark: SparkSession, table: String): DataFrame = {
    val host   = sys.env("TD_HOST")
    val secret = VaultClient.getSecret("pipelines/teradata")   // keys: username, password

    spark.read
      .format("jdbc")
      .option("url", s"jdbc:teradata://$host/CHARSET=UTF8,TMODE=ANSI")
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .option("user", secret("username"))
      .option("password", secret("password"))
      .option("dbtable", table)
      .load()
  }

  // ---------------- ORACLE ----------------
  def oracleRead(spark: SparkSession, table: String): DataFrame = {
    val host    = sys.env("ORA_HOST")
    val service = sys.env("ORA_SERVICE")
    val secret  = VaultClient.getSecret("pipelines/oracle")    // keys: username, password

    spark.read
      .format("jdbc")
      .option("url", s"jdbc:oracle:thin:@//$host:1521/$service")
      .option("driver", "oracle.jdbc.OracleDriver")
      .option("user", secret("username"))
      .option("password", secret("password"))
      .option("dbtable", table)
      .load()
  }

  // ---------------- SQL SERVER ----------------
  def sqlServerRead(spark: SparkSession, table: String): DataFrame = {
    val host     = sys.env("MSSQL_HOST")
    val database = sys.env("MSSQL_DB")
    val secret   = VaultClient.getSecret("pipelines/sqlserver") // keys: username, password

    spark.read
      .format("jdbc")
      .option("url", s"jdbc:sqlserver://$host:1433;databaseName=$database;encrypt=true;trustServerCertificate=true")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("user", secret("username"))
      .option("password", secret("password"))
      .option("dbtable", table)
      .load()
  }

  // ---------------- MINIO ----------------
  // path example: "bronze/orders"  ->  s3a://<bucket>/bronze/orders
  def minioRead(spark: SparkSession, path: String): DataFrame = {
    val endpoint = sys.env("MINIO_ENDPOINT")
    val bucket   = sys.env("MINIO_BUCKET")
    val secret   = VaultClient.getSecret("pipelines/minio")    // keys: access_key, secret_key

    val hadoop = spark.sparkContext.hadoopConfiguration
    hadoop.set("fs.s3a.endpoint", endpoint)
    hadoop.set("fs.s3a.access.key", secret("access_key"))
    hadoop.set("fs.s3a.secret.key", secret("secret_key"))
    hadoop.set("fs.s3a.path.style.access", "true")

    spark.read.parquet(s"s3a://$bucket/$path")
  }

  // ---------------- DATALAKE ----------------
  // path example: "raw/orders"  ->  <DATALAKE_BASE_PATH>/raw/orders
  // (no secret here: access is granted to the cluster identity, not a user/password)
  def datalakeRead(spark: SparkSession, path: String): DataFrame = {
    val basePath = sys.env("DATALAKE_BASE_PATH")   // e.g. hdfs://nn:8020/data

    spark.read.parquet(s"$basePath/$path")
  }
}
