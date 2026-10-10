#!/usr/bin/env python3
"""
spark_wrapper.py  --  launches ONE ini section as a Spark job on Kubernetes.

  python3 spark_wrapper.py --ini pipeline.ini --section orders_sql_td_to_minio \
                           --run-id 20261010T0200 --run-date 2026-10-10 [--dry-run]

What it does (in order)
  1. Reads [global] (Vault, connections, jars, image, git) + the task section from the ini
  2. Validates the task by job_type (sql / pyspark)
  3. job_type = sql  -> clones the git repo and loads the SQL file, fills ${params}
  4. T-shirt size    -> Spark driver/executor cores, memory, executors (built into this file)
  5. Load balancer   -> picks the Kubernetes namespace from k8s_lb.yaml
  6. Builds the job config map (source + target + params + connection details) as base64 JSON
  7. Builds spark-submit (image, service account, labels, env vars, resources, secrets via Vault)
  8. Runs it, streams logs, enforces timeout, cleans the driver pod on SIGTERM/timeout
  9. Returns the exit code to Airflow
"""
import argparse
import base64
import configparser
import datetime
import json
import logging
import os
import random
import re
import signal
import string
import subprocess
import sys
import tempfile
import threading
import uuid
from pathlib import Path

import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [wrapper] %(message)s")
log = logging.getLogger("fw.wrapper")

# ---------------------------------------------------------------------------
# Exit codes (Airflow shows these, so failures are easy to classify)
# ---------------------------------------------------------------------------
EXIT_CONFIG, EXIT_NO_CAPACITY, EXIT_GIT, EXIT_TIMEOUT = 2, 3, 4, 124


class WrapperError(Exception):
    def __init__(self, message, exit_code=EXIT_CONFIG):
        super().__init__(message)
        self.exit_code = exit_code


# ---------------------------------------------------------------------------
# 1. T-shirt sizes  (change here once -> every pipeline follows)
# ---------------------------------------------------------------------------
TSHIRT_SIZES = {
    "XS": dict(driver_cores=1, driver_mem="2g",  exec_cores=1, exec_mem="2g",  overhead="512m", min_exec=1, max_exec=2,  shuffle=16),
    "S":  dict(driver_cores=1, driver_mem="4g",  exec_cores=2, exec_mem="4g",  overhead="1g",   min_exec=1, max_exec=4,  shuffle=64),
    "M":  dict(driver_cores=2, driver_mem="6g",  exec_cores=4, exec_mem="8g",  overhead="1g",   min_exec=2, max_exec=10, shuffle=200),
    "L":  dict(driver_cores=4, driver_mem="8g",  exec_cores=4, exec_mem="16g", overhead="2g",   min_exec=4, max_exec=30, shuffle=600),
    "XL": dict(driver_cores=4, driver_mem="16g", exec_cores=5, exec_mem="24g", overhead="4g",   min_exec=8, max_exec=60, shuffle=1200),
}

# ---------------------------------------------------------------------------
# 2. Supported systems: which [global] keys they need, and which env vars the Scala jar reads
#    kind: db = needs source/target "table",  file = needs "path"
# ---------------------------------------------------------------------------
CONNECTIONS = {
    "TERADATA":  {"prefix": "teradata",  "kind": "db",   "required": ["host", "vault_path"],
                  "env": {"host": "TD_HOST", "port": "TD_PORT", "vault_path": "TD_VAULT_PATH"}},
    "ORACLE":    {"prefix": "oracle",    "kind": "db",   "required": ["host", "service", "vault_path"],
                  "env": {"host": "ORA_HOST", "port": "ORA_PORT", "service": "ORA_SERVICE", "vault_path": "ORA_VAULT_PATH"}},
    "SQLSERVER": {"prefix": "sqlserver", "kind": "db",   "required": ["host", "database", "vault_path"],
                  "env": {"host": "MSSQL_HOST", "port": "MSSQL_PORT", "database": "MSSQL_DB", "vault_path": "MSSQL_VAULT_PATH"}},
    "MINIO":     {"prefix": "minio",     "kind": "file", "required": ["endpoint", "bucket", "vault_path"],
                  "env": {"endpoint": "MINIO_ENDPOINT", "bucket": "MINIO_BUCKET", "vault_path": "MINIO_VAULT_PATH"}},
    "DATALAKE":  {"prefix": "datalake",  "kind": "file", "required": ["base_path"],
                  "env": {"base_path": "DATALAKE_BASE_PATH"}},
}

JOB_TYPES = {"sql", "pyspark"}
SQL_RUNNER_PATH = "local:///opt/app/jobs/sql_runner.py"


# ---------------------------------------------------------------------------
# 3. Reading and validating the ini
# ---------------------------------------------------------------------------
def normalise_type(value: str) -> str:
    return re.sub(r"[\s_-]", "", value or "").upper()          # "SQL Server" -> "SQLSERVER"


def subset(d: dict, prefix: str) -> dict:
    return {k[len(prefix):]: v for k, v in d.items() if k.startswith(prefix)}


def dns_name(text: str, max_len: int = 50) -> str:
    return re.sub(r"[^a-z0-9-]", "-", text.lower()).strip("-")[:max_len]


def load_ini(ini_path: str, section: str):
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";",))
    if not cp.read(ini_path):
        raise WrapperError(f"ini file not found: {ini_path}")
    if not cp.has_section("global"):
        raise WrapperError("ini must contain a [global] section")
    if section == "global" or not cp.has_section(section):
        tasks = [s for s in cp.sections() if s != "global"]
        raise WrapperError(f"task [{section}] not found. Tasks in file: {tasks}")
    return dict(cp["global"]), dict(cp[section])


def parse_task(g: dict, task: dict, section: str) -> dict:
    job_type = task.get("job_type", "").lower()
    if job_type not in JOB_TYPES:
        raise WrapperError(f"[{section}] job_type must be one of {sorted(JOB_TYPES)}, got '{job_type}'")

    size = task.get("tshirt_size", "").upper()
    if size not in TSHIRT_SIZES:
        raise WrapperError(f"[{section}] tshirt_size must be one of {list(TSHIRT_SIZES)}, got '{size}'")

    source, target = subset(task, "source."), subset(task, "target.")
    for side_name, side in (("source", source), ("target", target)):
        side["type"] = normalise_type(side.get("type"))
        if side["type"] not in CONNECTIONS:
            raise WrapperError(f"[{section}] {side_name}.type must be one of {sorted(CONNECTIONS)}")
        spec = CONNECTIONS[side["type"]]
        needed = "table" if spec["kind"] == "db" else "path"
        if not side.get(needed):
            raise WrapperError(f"[{section}] {side_name}.{needed} is required for {side['type']}")
        missing = [k for k in spec["required"] if not g.get(f"{spec['prefix']}.{k}")]
        if missing:
            raise WrapperError(f"[global] missing {[spec['prefix'] + '.' + k for k in missing]} (needed by {side_name})")

    if job_type == "sql" and not task.get("sql_git_path"):
        raise WrapperError(f"[{section}] job_type=sql needs sql_git_path")
    if job_type == "pyspark" and not task.get("script_path"):
        raise WrapperError(f"[{section}] job_type=pyspark needs script_path")
    for key in ("vault.addr", "vault.role", "docker.image", "jars"):
        if not g.get(key):
            raise WrapperError(f"[global] missing {key}")

    return {"job_type": job_type, "size": size, "source": source, "target": target,
            "params": subset(task, "param."), "spark_conf": subset(task, "spark_conf.")}


# ---------------------------------------------------------------------------
# 4. SQL from git
# ---------------------------------------------------------------------------
def fetch_sql(g: dict, task: dict, params: dict) -> str:
    repo = task.get("sql_git_repo") or g.get("git.repo_url")
    ref = task.get("sql_git_ref") or g.get("git.branch", "main")      # branch or tag
    if not repo:
        raise WrapperError("no git repo: set git.repo_url in [global] or sql_git_repo in the task")

    token = os.environ.get("GIT_TOKEN", "")                            # injected into the wrapper pod
    clone_url = repo.replace("https://", f"https://oauth2:{token}@", 1) if token and repo.startswith("https://") else repo

    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp) / "repo"
        res = subprocess.run(["git", "clone", "--quiet", "--depth", "1", "--branch", ref, clone_url, str(dest)],
                             capture_output=True, text=True, timeout=180)
        if res.returncode != 0:
            err = res.stderr.replace(token, "***") if token else res.stderr
            raise WrapperError(f"git clone failed ({repo} @ {ref}): {err.strip()}", EXIT_GIT)

        sql_file = (dest / task["sql_git_path"]).resolve()
        if dest.resolve() not in sql_file.parents or not sql_file.is_file():      # blocks ../ tricks
            raise WrapperError(f"SQL file not found in repo: {task['sql_git_path']}", EXIT_GIT)
        text = sql_file.read_text()

    return string.Template(text).safe_substitute(params)               # ${run_date}, ${country} ...


# ---------------------------------------------------------------------------
# 5. Load balancer (namespace picker)
# ---------------------------------------------------------------------------
def _k8s_api():
    from kubernetes import client, config                               # imported only when needed
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.CoreV1Api()


def running_drivers(namespace: str):
    """Number of Spark drivers Pending/Running in a namespace, or None if we cannot ask K8s."""
    try:
        pods = _k8s_api().list_namespaced_pod(namespace, label_selector="spark-role=driver")
        return sum(1 for p in pods.items if p.status.phase in ("Pending", "Running"))
    except Exception as exc:
        log.warning("cannot count drivers in %s (%s); treating as unknown", namespace, exc)
        return None


def pick_target(lb: dict, size: str, pinned: str = "", rng=random) -> dict:
    candidates = [t for t in lb["targets"]
                  if t.get("enabled", True) and size in t.get("allowed_sizes", list(TSHIRT_SIZES))]
    if pinned:
        candidates = [t for t in candidates if t["name"] == pinned]
    if not candidates:
        raise WrapperError(f"no enabled namespace for size {size} (pinned='{pinned}')", EXIT_NO_CAPACITY)

    strategy = lb.get("strategy", "weighted_random")
    load = {}
    if strategy == "least_loaded" or any(t.get("max_running_jobs") for t in candidates):
        load = {t["name"]: running_drivers(t["namespace"]) for t in candidates}

    # capacity filter
    open_targets = [t for t in candidates
                    if not t.get("max_running_jobs") or load.get(t["name"]) is None
                    or load[t["name"]] < t["max_running_jobs"]]
    if not open_targets:
        raise WrapperError(f"all namespaces for size {size} are at max_running_jobs: {load}", EXIT_NO_CAPACITY)

    if strategy == "least_loaded" and all(load.get(t["name"]) is not None for t in open_targets):
        return min(open_targets, key=lambda t: load[t["name"]] / max(t.get("weight", 1), 1))
    return rng.choices(open_targets, weights=[max(t.get("weight", 1), 1) for t in open_targets], k=1)[0]


def delete_driver_pod(namespace: str, pod_name: str):
    try:
        _k8s_api().delete_namespaced_pod(pod_name, namespace)
        log.info("deleted driver pod %s/%s", namespace, pod_name)
    except Exception as exc:
        log.warning("could not delete driver pod %s/%s: %s", namespace, pod_name, exc)


# ---------------------------------------------------------------------------
# 6. Job config map + container environment
# ---------------------------------------------------------------------------
def build_job_config(parsed: dict, section: str, run_id: str, sql_text: str) -> str:
    """Non-secret config map handed to the driver script (--config <base64 json>)."""
    cfg = {"section": section, "run_id": run_id, "job_type": parsed["job_type"],
           "source_type": parsed["source"]["type"], "target_type": parsed["target"]["type"],
           "source": parsed["source"], "target": parsed["target"],
           "params": parsed["params"], "sql": sql_text}
    return base64.b64encode(json.dumps(cfg).encode()).decode()


def build_env(g: dict, parsed: dict) -> dict:
    """Env vars for driver + executors: Vault coordinates and ONLY the systems this job uses."""
    env = {"VAULT_ADDR": g["vault.addr"], "VAULT_ROLE": g["vault.role"], "VAULT_MOUNT": g.get("vault.mount", "secret")}
    if g.get("vault.namespace"):
        env["VAULT_NAMESPACE"] = g["vault.namespace"]
    for system in {parsed["source"]["type"], parsed["target"]["type"]}:
        spec = CONNECTIONS[system]
        for key, env_name in spec["env"].items():
            value = g.get(f"{spec['prefix']}.{key}")
            if value:
                env[env_name] = value
    return env


# ---------------------------------------------------------------------------
# 7. spark-submit builder
# ---------------------------------------------------------------------------
def build_spark_submit(g, task, parsed, target, section, run_id, config_b64):
    s = TSHIRT_SIZES[parsed["size"]]
    app_name = dns_name(f"fw-{section}-{run_id}")
    ns = target["namespace"]
    jars = ",".join(j for j in [g["jars"], task.get("extra_jars", "")] if j)
    app_file = SQL_RUNNER_PATH if parsed["job_type"] == "sql" else task["script_path"]

    conf = {
        # ---- identity / placement ----
        "spark.app.name": app_name,
        "spark.kubernetes.namespace": ns,
        "spark.kubernetes.container.image": g["docker.image"],
        "spark.kubernetes.container.image.pullPolicy": g.get("k8s.image_pull_policy", "IfNotPresent"),
        "spark.kubernetes.authenticate.driver.serviceAccountName": g.get("k8s.driver_service_account", "spark-driver"),
        "spark.kubernetes.driver.pod.name": app_name,
        "spark.kubernetes.submission.waitAppCompletion": "true",
        "spark.kubernetes.executor.deleteOnTermination": "true",
        # ---- t-shirt resources (request == limit, so pods get Guaranteed-style CPU) ----
        "spark.driver.cores": s["driver_cores"],
        "spark.driver.memory": s["driver_mem"],
        "spark.kubernetes.driver.request.cores": s["driver_cores"],
        "spark.kubernetes.driver.limit.cores": s["driver_cores"],
        "spark.executor.cores": s["exec_cores"],
        "spark.executor.memory": s["exec_mem"],
        "spark.executor.memoryOverhead": s["overhead"],
        "spark.kubernetes.executor.request.cores": s["exec_cores"],
        "spark.kubernetes.executor.limit.cores": s["exec_cores"],
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
        "spark.dynamicAllocation.minExecutors": s["min_exec"],
        "spark.dynamicAllocation.initialExecutors": s["min_exec"],
        "spark.dynamicAllocation.maxExecutors": s["max_exec"],
        "spark.sql.shuffle.partitions": s["shuffle"],
        "spark.sql.adaptive.enabled": "true",
    }
    if g.get("k8s.image_pull_secret"):
        conf["spark.kubernetes.container.image.pullSecrets"] = g["k8s.image_pull_secret"]
    if g.get("spark.event_log_dir"):
        conf["spark.eventLog.enabled"] = "true"
        conf["spark.eventLog.dir"] = g["spark.event_log_dir"]

    # ---- labels + annotations on driver and executors (filter pods/logs per DAG task) ----
    labels = {"fw-section": dns_name(section, 60), "fw-job-type": parsed["job_type"],
              "fw-tshirt": parsed["size"].lower(), "fw-run-id": dns_name(run_id, 60)}
    for role in ("driver", "executor"):
        for k, v in labels.items():
            conf[f"spark.kubernetes.{role}.label.{k}"] = v
        if g.get("docker.git_repo"):
            conf[f"spark.kubernetes.{role}.annotation.acme.com/image-git"] = f"{g['docker.git_repo']}:{g.get('docker.git_path', '')}"

    # ---- node pool from the load balancer ----
    for k, v in (target.get("node_selector") or {}).items():
        conf[f"spark.kubernetes.node.selector.{k}"] = v

    # ---- env vars in driver + executors ----
    for k, v in build_env(g, parsed).items():
        conf[f"spark.kubernetes.driverEnv.{k}"] = v
        conf[f"spark.executorEnv.{k}"] = v

    conf.update(parsed["spark_conf"])                                   # per-task overrides win

    cmd = ["spark-submit", "--master", target["master"], "--deploy-mode", "cluster",
           "--name", app_name, "--jars", jars]
    for k, v in conf.items():
        cmd += ["--conf", f"{k}={v}"]
    cmd += [app_file, "--config", config_b64]
    return cmd, app_name


# ---------------------------------------------------------------------------
# 8. Run + log streaming + timeout + cleanup
# ---------------------------------------------------------------------------
def run_spark_submit(cmd, timeout_min: int, namespace: str, pod_name: str) -> int:
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    state = {"timed_out": False}

    def on_timeout():
        state["timed_out"] = True
        log.error("timeout after %s min: stopping job", timeout_min)
        proc.terminate()
        delete_driver_pod(namespace, pod_name)

    def on_signal(signum, _frame):                                      # Airflow killed the task
        log.warning("received signal %s: stopping job", signum)
        proc.terminate()
        delete_driver_pod(namespace, pod_name)
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    timer = threading.Timer(timeout_min * 60, on_timeout)
    timer.start()
    try:
        for line in proc.stdout:                                        # stream to Airflow task log
            print(line, end="")
        rc = proc.wait()
    finally:
        timer.cancel()
    return EXIT_TIMEOUT if state["timed_out"] else rc


# ---------------------------------------------------------------------------
# 9. main
# ---------------------------------------------------------------------------
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Run one ini section as a Spark job on Kubernetes")
    ap.add_argument("--ini", required=True)
    ap.add_argument("--section", required=True)
    ap.add_argument("--lb-file", default=str(Path(__file__).with_name("k8s_lb.yaml")))
    ap.add_argument("--run-id", default=uuid.uuid4().hex[:10])
    ap.add_argument("--run-date", default=datetime.date.today().isoformat())
    ap.add_argument("--dry-run", action="store_true", help="print the spark-submit and config, run nothing")
    args = ap.parse_args(argv)

    try:
        run_id = dns_name(args.run_id, 20) or uuid.uuid4().hex[:10]
        g, task = load_ini(args.ini, args.section)
        parsed = parse_task(g, task, args.section)

        params = dict(parsed["params"], run_id=run_id, run_date=args.run_date, section=args.section)
        sql_text = fetch_sql(g, task, params) if parsed["job_type"] == "sql" else ""

        with open(args.lb_file) as fh:
            lb = yaml.safe_load(fh)
        target = pick_target(lb, parsed["size"], task.get("lb_target", ""))
        log.info("section=%s type=%s size=%s -> namespace=%s",
                 args.section, parsed["job_type"], parsed["size"], target["namespace"])

        config_b64 = build_job_config(parsed, args.section, run_id, sql_text)
        cmd, pod_name = build_spark_submit(g, task, parsed, target, args.section, run_id, config_b64)

        if args.dry_run:
            print(" \\\n  ".join(cmd[:-1]) + " \\\n  <config>")
            print("\n--- job config map ---")
            print(json.dumps(json.loads(base64.b64decode(config_b64)), indent=2))
            return 0

        timeout = int(task.get("timeout_minutes") or g.get("default_timeout_minutes", 240))
        rc = run_spark_submit(cmd, timeout, target["namespace"], pod_name)
        log.info("spark-submit finished with exit code %s", rc)
        return rc

    except WrapperError as exc:
        log.error("%s", exc)
        return exc.exit_code


if __name__ == "__main__":
    sys.exit(main())
