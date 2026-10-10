"""
Simple Airflow DAG.
Each Airflow task = ONE (ini file + section name) pair.
The task starts a small K8s pod that runs:  spark_wrapper.py --ini <ini_file> --section <section>
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

IMAGE = "registry.acme.com/data/spark-fw:1.4.0"     # image that contains the wrapper + ini files
CONF_DIR = "/opt/app/conf"                          # where the ini files live inside the image


def spark_task(task_id: str, ini_file: str, section: str) -> KubernetesPodOperator:
    """One Airflow task for one section of one ini file."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace="airflow-jobs",
        kubernetes_conn_id="k8s_default",             # Airflow connection to the K8s cluster
        image=IMAGE,
        cmds=["python3", "/opt/app/wrapper/spark_wrapper.py"],
        arguments=["--ini", f"{CONF_DIR}/{ini_file}",
                   "--section", section,
                   "--run-id", "{{ ts_nodash }}",
                   "--run-date", "{{ ds }}"],
        service_account_name="spark-submitter",
        get_logs=True,
        on_finish_action="delete_pod",
    )


with DAG(
    dag_id="orders_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    #                       task name       ini file          section name
    source_task = spark_task("source_task", "source.ini",     "extract_orders")
    stage_task  = spark_task("stage_task",  "stage.ini",      "stage_orders")
    target_task = spark_task("target_task", "target.ini",     "load_orders")

    source_task >> stage_task >> target_task
