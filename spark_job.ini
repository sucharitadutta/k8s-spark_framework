; =====================================================================
;  ONE ini FILE = ONE Airflow DAG      ONE [section] = ONE Airflow task
;  [global] is NOT a task: it holds Vault, connections, jars, image, git
;  NO passwords here. user/password live in Vault; the ini only holds the Vault PATH.
; =====================================================================

[global]
; ---------- Vault ----------
vault.addr                = https://vault.acme.com:8200
vault.namespace           = data-platform
vault.mount               = secret
vault.role                = spark-driver

; ---------- Source / target systems (endpoint details; credentials come from Vault) ----------
teradata.host             = 10.20.30.40
teradata.port             = 1025
teradata.vault_path       = pipelines/teradata        ; keys inside: username, password

oracle.host               = 10.20.30.50
oracle.port               = 1521
oracle.service            = ORCLPDB1
oracle.vault_path         = pipelines/oracle          ; keys inside: username, password

sqlserver.host            = 10.20.30.60
sqlserver.port            = 1433
sqlserver.database        = SalesDW
sqlserver.vault_path      = pipelines/sqlserver       ; keys inside: username, password

minio.endpoint            = https://minio.acme.com
minio.bucket              = lake
minio.vault_path          = pipelines/minio           ; keys inside: access_key, secret_key

datalake.base_path        = abfss://data@acmelake.dfs.core.windows.net

; ---------- Jars (framework jar + JDBC drivers + s3a), baked in the image ----------
jars                      = local:///opt/app/jars/fw-connectors-1.4.0.jar,local:///opt/app/jars/terajdbc4.jar,local:///opt/app/jars/ojdbc11.jar,local:///opt/app/jars/mssql-jdbc.jar,local:///opt/app/jars/hadoop-aws-3.3.4.jar,local:///opt/app/jars/aws-java-sdk-bundle-1.12.262.jar

; ---------- Docker image (built from git by CI) ----------
docker.image              = registry.acme.com/data/spark-fw:1.4.0
docker.git_repo           = https://git.acme.com/data/spark-fw-image.git
docker.git_path           = docker/Dockerfile

; ---------- Git repo that holds the SQL files ----------
git.repo_url              = https://git.acme.com/data/sql-pipelines.git
git.branch                = main

; ---------- Kubernetes pod settings (namespace itself is chosen by k8s_lb.yaml) ----------
k8s.driver_service_account = spark-driver
k8s.image_pull_secret      = regcred
k8s.image_pull_policy      = IfNotPresent
spark.event_log_dir        = s3a://lake/spark-events
default_timeout_minutes    = 240


; =====================================================================
;  TASKS
; =====================================================================

; ---- job_type = sql : SQL file from git, run on a source view, result written to target ----
[orders_sql_td_to_minio]
job_type        = sql
tshirt_size     = M
sql_git_path    = sql/orders/curate_orders.sql
depends_on      =
; source config map  (view name = the table name to use inside the SQL file)
source.type     = TERADATA
source.table    = SALES.ORDERS
source.view     = orders
; target config map
target.type     = MINIO
target.path     = silver/orders
; values usable inside the SQL as ${run_date}, ${country}
param.country   = IN

; ---- job_type = pyspark : your own script (baked in image), config map passed to it ----
[orders_pyspark_enrich]
job_type        = pyspark
tshirt_size     = L
script_path     = local:///opt/app/jobs/orders_enrich.py
depends_on      = orders_sql_td_to_minio
source.type     = MINIO
source.path     = silver/orders
target.type     = ORACLE
target.table    = MART.ORDERS_ENRICHED
param.min_amount = 100
; optional per-job Spark overrides and pinning to one LB target
spark_conf.spark.sql.shuffle.partitions = 800
lb_target       =
