# Apache Airflow — Exhaustive Reference Card

> **Version target:** Airflow 2.x (2.7+ where noted) | Python 3.9+  
> **Quick nav:** [Core Concepts](#core-concepts) · [DAG API](#dag-api) · [Operators](#operators) · [Sensors](#sensors) · [Hooks](#hooks) · [TaskFlow API](#taskflow-api) · [XComs](#xcoms) · [Connections & Variables](#connections--variables) · [Scheduling](#scheduling) · [Dynamic DAGs](#dynamic-dags) · [Branching](#branching) · [Dependencies](#dependencies) · [Datasets](#datasets) · [Setup & Config](#setup--config) · [CLI](#cli-reference) · [Best Practices](#best-practices) · [Design Patterns](#design-patterns)

---

## Core Concepts

| Term | Description |
|---|---|
| **DAG** | Directed Acyclic Graph — the workflow definition |
| **Task** | A unit of work within a DAG |
| **Operator** | Template for a task (defines what it does) |
| **Sensor** | Operator that waits for a condition to be true |
| **Hook** | Interface to an external service (DB, API, etc.) |
| **XCom** | Cross-communication — passing data between tasks |
| **Connection** | Stored credentials for external systems |
| **Variable** | Global key-value store for configuration |
| **TaskInstance (TI)** | A specific run of a task |
| **DagRun** | A specific execution of a DAG |
| **Executor** | Component that runs tasks (LocalExecutor, CeleryExecutor, etc.) |
| **Scheduler** | Airflow process that triggers DAG runs |
| **Webserver** | Airflow UI (Flask-based) |
| **Metadata DB** | Stores all state (Postgres/MySQL recommended for prod) |
| **Pool** | Limit concurrent task slots |
| **Queue** | Route tasks to specific workers (Celery) |
| **Priority Weight** | Task scheduling priority |
| **SLA** | Service Level Agreement — alert if task misses deadline |

---

## DAG API

### DAG Declaration — Classic Style

```python
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    "owner":            "tobias",
    "depends_on_past":  False,
    "email":            ["alert@example.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":  timedelta(minutes=60),
    "execution_timeout": timedelta(hours=1),
    "sla":              timedelta(hours=2),
}

with DAG(
    dag_id="my_pipeline",
    description="Example production DAG",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",          # cron or timedelta or None
    catchup=False,                  # !! always False unless backfill needed
    max_active_runs=1,              # prevent overlapping runs
    max_active_tasks=16,
    tags=["etl", "production"],
    doc_md=__doc__,                 # renders in UI
    render_template_as_native_obj=False,
    template_searchpath=["/opt/airflow/sql"],
    params={"env": "prod"},         # runtime params (Airflow 2.2+)
    on_failure_callback=notify_slack,
    on_success_callback=None,
    dagrun_timeout=timedelta(hours=4),
) as dag:
    ...
```

### DAG Declaration — Decorator Style (TaskFlow)

```python
from airflow.decorators import dag
from datetime import datetime

@dag(
    dag_id="taskflow_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["taskflow"],
)
def taskflow_pipeline():
    ...

dag_instance = taskflow_pipeline()
```

### DAG Parameters Reference

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dag_id` | str | required | Unique identifier |
| `description` | str | `None` | Human-readable description |
| `schedule` / `schedule_interval` | str/timedelta/Dataset | `None` | Trigger interval |
| `start_date` | datetime | required | First logical date |
| `end_date` | datetime | `None` | Stop scheduling after |
| `catchup` | bool | `True` | Backfill past runs |
| `max_active_runs` | int | config | Max concurrent DagRuns |
| `max_active_tasks` | int | config | Max tasks running simultaneously |
| `default_args` | dict | `{}` | Defaults passed to all tasks |
| `tags` | list[str] | `[]` | UI filter tags |
| `params` | dict | `{}` | Runtime param definitions |
| `render_template_as_native_obj` | bool | `False` | Return Python objects from Jinja |
| `template_searchpath` | str/list | `None` | Extra dirs for template files |
| `on_failure_callback` | callable | `None` | Called on DAG failure |
| `on_success_callback` | callable | `None` | Called on DAG success |
| `dagrun_timeout` | timedelta | `None` | Max DagRun duration |
| `doc_md` | str | `None` | Markdown docs shown in UI |
| `access_control` | dict | `None` | Role-based access per DAG |
| `is_paused_upon_creation` | bool | config | Start paused |
| `orientation` | str | `"LR"` | Graph view (`"LR"` or `"TB"`) |
| `concurrency` | int | config | (deprecated → `max_active_tasks`) |

---

## Operators

### BaseOperator — All Parameters

Every operator inherits these from `BaseOperator`:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `task_id` | str | required | Unique task identifier within DAG |
| `dag` | DAG | context | Parent DAG |
| `owner` | str | `"airflow"` | Task owner |
| `depends_on_past` | bool | `False` | Only run if previous TI succeeded |
| `wait_for_downstream` | bool | `False` | Wait for downstream of previous run |
| `retries` | int | `0` | Retry count on failure |
| `retry_delay` | timedelta | `5min` | Delay between retries |
| `retry_exponential_backoff` | bool | `False` | Exponential retry delays |
| `max_retry_delay` | timedelta | `None` | Cap on exponential backoff |
| `start_date` | datetime | DAG default | Override task start date |
| `end_date` | datetime | `None` | Override task end date |
| `execution_timeout` | timedelta | `None` | Max task run duration |
| `on_failure_callback` | callable | `None` | Task failure hook |
| `on_success_callback` | callable | `None` | Task success hook |
| `on_retry_callback` | callable | `None` | Task retry hook |
| `on_skipped_callback` | callable | `None` | Task skip hook (2.7+) |
| `email` | str/list | `None` | Alert email |
| `email_on_failure` | bool | `True` | Email on failure |
| `email_on_retry` | bool | `True` | Email on retry |
| `trigger_rule` | str | `"all_success"` | Dependency rule |
| `resources` | dict | `None` | CPU/RAM constraints (KubernetesExecutor) |
| `run_as_user` | str | `None` | Unix user for task |
| `task_concurrency` | int | `None` | (deprecated → `max_active_tis_per_dag`) |
| `max_active_tis_per_dag` | int | `None` | Limit concurrent TIs across all runs |
| `pool` | str | `"default_pool"` | Resource pool |
| `pool_slots` | int | `1` | Slots consumed from pool |
| `queue` | str | `"default"` | Celery queue |
| `priority_weight` | int | `1` | Higher = scheduled first |
| `weight_rule` | str | `"downstream"` | How weight propagates |
| `sla` | timedelta | `None` | SLA deadline |
| `do_xcom_push` | bool | `True` | Auto-push return value to XCom |
| `multiple_outputs` | bool | `False` | Unpack dict return into multiple XComs |
| `doc` / `doc_md` | str | `None` | Task documentation |
| `inlets` / `outlets` | list | `[]` | Data lineage tracking |

### Trigger Rules

| Rule | When task runs |
|---|---|
| `all_success` | All upstream succeeded (default) |
| `all_failed` | All upstream failed |
| `all_done` | All upstream finished (any state) |
| `all_skipped` | All upstream skipped (2.3+) |
| `one_success` | At least one upstream succeeded |
| `one_failed` | At least one upstream failed |
| `one_done` | At least one upstream finished |
| `none_failed` | No upstream failed (success or skipped OK) |
| `none_failed_min_one_success` | None failed AND at least one succeeded |
| `none_skipped` | No upstream skipped |
| `always` | Always run regardless of upstream |
| `dummy` | (deprecated alias for `always`) |

```python
from airflow.utils.trigger_rule import TriggerRule

cleanup = EmptyOperator(
    task_id="cleanup",
    trigger_rule=TriggerRule.ALL_DONE,
)
```

### Common Built-in Operators

```python
from airflow.operators.bash      import BashOperator
from airflow.operators.python    import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.empty     import EmptyOperator          # no-op, replaces DummyOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.operators.email     import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.subdag    import SubDagOperator         # deprecated — prefer TaskGroups

# BashOperator
run_script = BashOperator(
    task_id="run_script",
    bash_command="python /opt/airflow/scripts/process.py --date {{ ds }}",
    env={"MY_VAR": "value"},
    cwd="/opt/airflow",
    append_env=True,
)

# PythonOperator
def process_data(ds, **kwargs):
    ti = kwargs["ti"]
    return {"records": 42}

process = PythonOperator(
    task_id="process",
    python_callable=process_data,
    op_kwargs={"extra_param": "value"},  # additional kwargs
    op_args=[],
    templates_dict={"date": "{{ ds }}"},  # Jinja-rendered dict injected as templates_dict
    provide_context=True,                  # deprecated in 2.x (always True now)
)

# EmptyOperator (start/end markers)
start = EmptyOperator(task_id="start")
end   = EmptyOperator(task_id="end")

# EmailOperator
send_report = EmailOperator(
    task_id="send_report",
    to=["team@example.com"],
    subject="Pipeline complete {{ ds }}",
    html_content="<h1>Done</h1>",
    files=["/opt/airflow/reports/report_{{ ds }}.csv"],
)

# TriggerDagRunOperator
trigger_child = TriggerDagRunOperator(
    task_id="trigger_child",
    trigger_dag_id="child_pipeline",
    conf={"parent_run_id": "{{ run_id }}"},
    wait_for_completion=True,
    poke_interval=30,
    reset_dag_run=True,
)
```

### Database Operators

```python
# PostgreSQL
from airflow.providers.postgres.operators.postgres import PostgresOperator

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_default",
    sql="sql/create_table.sql",       # file path (searched in template_searchpath)
    # sql="SELECT 1",                 # or inline string
    parameters={"schema": "public"},
    autocommit=False,
    database="mydb",
)

# Generic SQL (provider-agnostic via SQLExecuteQueryOperator — Airflow 2.5+)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

run_sql = SQLExecuteQueryOperator(
    task_id="run_sql",
    conn_id="postgres_default",
    sql="SELECT COUNT(*) FROM {{ params.table }}",
    return_last=True,
    split_statements=True,
)
```

### HTTP Operator

```python
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

call_api = SimpleHttpOperator(
    task_id="call_api",
    http_conn_id="my_api",
    endpoint="v1/data",
    method="POST",
    data=json.dumps({"date": "{{ ds }}"}),
    headers={"Content-Type": "application/json"},
    response_filter=lambda r: r.json()["result"],
    log_response=True,
)
```

### Databricks Operators

```python
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
    DatabricksCreateJobsOperator,
)

# Trigger an existing Databricks Job
trigger_job = DatabricksRunNowOperator(
    task_id="trigger_job",
    databricks_conn_id="databricks_default",
    job_id=12345,
    notebook_params={"env": "prod", "date": "{{ ds }}"},
    idempotency_token="{{ run_id }}",
)

# Submit an ad-hoc run
submit_run = DatabricksSubmitRunOperator(
    task_id="submit_run",
    databricks_conn_id="databricks_default",
    existing_cluster_id="0101-abc123",
    notebook_task={
        "notebook_path": "/Repos/project/notebook",
        "base_parameters": {"date": "{{ ds }}"},
    },
)
```

---

## Sensors

```python
from airflow.sensors.base         import BaseSensorOperator
from airflow.sensors.filesystem   import FileSensor
from airflow.sensors.python       import PythonSensor
from airflow.sensors.time_delta   import TimeDeltaSensor, TimeDeltaSensorAsync
from airflow.sensors.date_time    import DateTimeSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# FileSensor
wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath="/data/landing/{{ ds }}/file.csv",
    poke_interval=60,       # seconds between checks
    timeout=3600,           # fail after N seconds
    mode="reschedule",      # !! use reschedule to free worker slots
    soft_fail=False,        # True = skip instead of fail on timeout
)

# ExternalTaskSensor — wait for another DAG's task
wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_upstream",
    external_dag_id="upstream_pipeline",
    external_task_id="final_task",        # None = wait for whole DAG
    execution_delta=timedelta(hours=1),   # offset in logical dates
    check_existence=True,
    mode="reschedule",
    timeout=7200,
)

# HttpSensor
api_ready = HttpSensor(
    task_id="api_ready",
    http_conn_id="my_api",
    endpoint="health",
    request_params={},
    response_check=lambda response: response.json()["status"] == "ok",
    poke_interval=30,
    mode="reschedule",
)

# S3KeySensor
s3_file = S3KeySensor(
    task_id="s3_file",
    bucket_name="my-bucket",
    bucket_key="data/{{ ds }}/file.parquet",
    aws_conn_id="aws_default",
    mode="reschedule",
)

# Custom Sensor
class MyApiSensor(BaseSensorOperator):
    def __init__(self, endpoint: str, **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint

    def poke(self, context) -> bool:
        import requests
        r = requests.get(self.endpoint)
        return r.status_code == 200 and r.json().get("ready") is True
```

### Sensor Modes

| Mode | Description | Use case |
|---|---|---|
| `poke` | Occupies worker slot between checks | Short wait times (< 5 min) |
| `reschedule` | Releases slot between checks | Long waits — **prefer this** |
| `deferrable` | Async with Triggerer process | Most efficient (Airflow 2.2+) |

### Deferrable Operators/Sensors (Airflow 2.2+)

```python
from airflow.providers.http.sensors.http import HttpSensor

# Use async variant — requires Triggerer service
wait_async = TimeDeltaSensorAsync(
    task_id="wait_async",
    delta=timedelta(minutes=30),
)
# Many providers ship *Async variants; look for them to save worker slots
```

---

## Hooks

Hooks abstract the connection logic — operators use hooks internally.

```python
from airflow.hooks.base                          import BaseHook
from airflow.providers.postgres.hooks.postgres   import PostgresHook
from airflow.providers.http.hooks.http           import HttpHook
from airflow.providers.amazon.aws.hooks.s3       import S3Hook

# PostgresHook
def query_postgres(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records("SELECT id, name FROM users LIMIT 10")
    df = hook.get_pandas_df("SELECT * FROM orders WHERE date = %s", parameters=["2024-01-01"])
    hook.run("INSERT INTO log VALUES (%s)", parameters=["event"])
    hook.copy_expert("COPY table FROM STDIN CSV HEADER", "/tmp/data.csv")
    return records

# S3Hook
def upload_to_s3(**kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(filename="/tmp/data.csv", key="data/file.csv", bucket_name="my-bucket")
    hook.load_string("content", key="data/text.txt", bucket_name="my-bucket")
    keys = hook.list_keys(bucket_name="my-bucket", prefix="data/")
    exists = hook.check_for_key(key="data/file.csv", bucket_name="my-bucket")

# HttpHook
def call_rest_api(**kwargs):
    hook = HttpHook(http_conn_id="my_api", method="GET")
    response = hook.run(endpoint="v1/data", data={"date": "2024-01-01"})
    return response.json()

# Generic connection access
def get_any_connection():
    conn = BaseHook.get_connection("my_conn_id")
    print(conn.host, conn.login, conn.password, conn.port, conn.schema)
    extra = conn.extra_dejson  # parsed JSON extras
```

### PostgresHook Method Table

| Method | Signature | Description |
|---|---|---|
| `get_conn()` | `()` | Returns `psycopg2` connection |
| `get_records()` | `(sql, parameters=None)` | List of tuples |
| `get_first()` | `(sql, parameters=None)` | First row as tuple |
| `get_pandas_df()` | `(sql, parameters=None)` | `pd.DataFrame` |
| `run()` | `(sql, autocommit=False, parameters=None)` | Execute statement |
| `copy_expert()` | `(sql, filename)` | Bulk COPY |
| `bulk_load()` | `(table, tmp_file)` | Tab-delimited bulk load |
| `insert_rows()` | `(table, rows, target_fields, commit_every)` | Batch insert |

---

## TaskFlow API

The **TaskFlow API** (Airflow 2.0+) uses decorators to define tasks as Python functions, with automatic XCom passing.

```python
from airflow.decorators import dag, task, task_group
from datetime import datetime

@dag(start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False)
def etl_pipeline():

    @task()
    def extract() -> dict:
        return {"data": [1, 2, 3], "count": 3}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def transform(raw: dict) -> list:
        return [x * 2 for x in raw["data"]]

    @task(pool="db_pool", pool_slots=2)
    def load(records: list) -> None:
        print(f"Loading {len(records)} records")

    # Automatic XCom: return values passed directly
    raw = extract()
    processed = transform(raw)
    load(processed)

dag = etl_pipeline()
```

### TaskFlow — Multiple Outputs

```python
@task(multiple_outputs=True)   # unpacks dict into separate XCom keys
def get_config() -> dict:
    return {"host": "db.example.com", "port": 5432}

config = get_config()
# config["host"] and config["port"] are separate XComs
```

### TaskFlow — Virtualenv & ExternalPython

```python
from airflow.decorators import task

@task.virtualenv(
    task_id="pandas_task",
    requirements=["pandas==2.0.3", "pyarrow"],
    system_site_packages=False,
    python_version="3.10",
)
def pandas_task():
    import pandas as pd
    df = pd.DataFrame({"a": [1, 2, 3]})
    return df.to_dict()

@task.external_python(
    task_id="external_task",
    python="/opt/venv/bin/python",
)
def external_task():
    import numpy as np
    return float(np.mean([1, 2, 3]))
```

### TaskFlow — Branch

```python
from airflow.decorators import task

@task.branch()
def choose_path(condition: bool) -> str:
    return "task_a" if condition else "task_b"
```

### TaskGroups

```python
from airflow.utils.task_group import TaskGroup

with DAG("grouped", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:
    with TaskGroup("ingestion", tooltip="Data ingestion tasks") as ingest:
        extract = BashOperator(task_id="extract", bash_command="echo extract")
        validate = BashOperator(task_id="validate", bash_command="echo validate")
        extract >> validate

    with TaskGroup("transform") as transform:
        clean   = BashOperator(task_id="clean",   bash_command="echo clean")
        enrich  = BashOperator(task_id="enrich",  bash_command="echo enrich")
        clean >> enrich

    ingest >> transform

# TaskGroup decorator (TaskFlow)
@task_group(group_id="loading")
def loading_group():
    @task
    def load_raw(): pass
    @task
    def load_curated(): pass
    load_raw() >> load_curated()
```

---

## XComs

XComs (cross-communications) pass data between tasks. Stored in the metadata DB — **keep small** (< 48KB recommended).

```python
# Push XCom
def push_data(**kwargs):
    kwargs["ti"].xcom_push(key="my_key", value={"count": 42})

# Pull XCom
def pull_data(**kwargs):
    ti = kwargs["ti"]
    value = ti.xcom_pull(task_ids="push_task", key="my_key")
    # Pull from multiple tasks
    values = ti.xcom_pull(task_ids=["task_a", "task_b"], key="result")

# Auto-push via return value (do_xcom_push=True by default)
def auto_push(**kwargs):
    return "this gets pushed as key='return_value'"

# Jinja template XCom access
task_b = BashOperator(
    task_id="task_b",
    bash_command="echo {{ ti.xcom_pull(task_ids='task_a') }}",
)
```

### XCom Backend (large data)

For large payloads, use a custom XCom backend (e.g., S3/GCS):

```python
# airflow.cfg or env var:
# AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.amazon.aws.xcom_backends.s3.S3XComBackend

# Or custom:
from airflow.models.xcom import BaseXCom

class S3XComBackend(BaseXCom):
    PREFIX = "xcom_s3://"

    @staticmethod
    def serialize_value(value, **kwargs):
        import pickle, boto3, uuid
        key = f"xcom/{uuid.uuid4()}.pkl"
        boto3.client("s3").put_object(Bucket="my-bucket", Key=key, Body=pickle.dumps(value))
        return BaseXCom.serialize_value(f"{S3XComBackend.PREFIX}{key}")

    @staticmethod
    def deserialize_value(result):
        import pickle, boto3
        val = BaseXCom.deserialize_value(result)
        if str(val).startswith(S3XComBackend.PREFIX):
            key = val[len(S3XComBackend.PREFIX):]
            obj = boto3.client("s3").get_object(Bucket="my-bucket", Key=key)
            return pickle.loads(obj["Body"].read())
        return val
```

---

## Connections & Variables

### Connections

```python
# airflow.cfg / environment variable approach (recommended for secrets):
# AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://user:pass@host:5432/db
# AIRFLOW_CONN_MY_API=http://user:token@api.example.com/

# Programmatic access
from airflow.models import Connection
from airflow import settings

# Create connection via code
conn = Connection(
    conn_id="my_db",
    conn_type="postgres",
    host="localhost",
    login="user",
    password="secret",
    port=5432,
    schema="mydb",
    extra='{"sslmode": "require"}',
)
session = settings.Session()
session.add(conn)
session.commit()

# Access in a task
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("my_db")
uri  = conn.get_uri()
```

### Variables

```python
from airflow.models import Variable

# Set
Variable.set("batch_size", "1000")
Variable.set("config", {"env": "prod", "retries": 3}, serialize_json=True)

# Get
batch_size = Variable.get("batch_size", default_var="500")
config     = Variable.get("config", deserialize_json=True)

# Jinja template (cached per DagRun — don't use for secrets)
cmd = BashOperator(
    task_id="cmd",
    bash_command="echo {{ var.value.batch_size }}",
    # or: {{ var.json.config.env }}
)
```

### Secrets Backends

```python
# airflow.cfg
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"connections_path": "airflow/connections", "variables_path": "airflow/variables", "url": "http://vault:8200"}

# AWS Secrets Manager
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
# Azure Key Vault
backend = airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
```

---

## Scheduling

### Cron Presets

| Preset | Cron | Description |
|---|---|---|
| `@once` | — | Run once only |
| `@hourly` | `0 * * * *` | Every hour |
| `@daily` | `0 0 * * *` | Midnight daily |
| `@weekly` | `0 0 * * 0` | Sunday midnight |
| `@monthly` | `0 0 1 * *` | 1st of month |
| `@quarterly` | `0 0 1 */3 *` | Quarterly |
| `@yearly` | `0 0 1 1 *` | Jan 1st |
| `None` | — | Manual trigger only |

### Timetables (Airflow 2.2+)

```python
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.trigger  import CronTriggerTimetable

# Custom timetable (e.g., business days only)
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from pendulum import DateTime, instance
import pendulum

class BusinessDayTimetable(Timetable):
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        start = run_after.start_of("day")
        return DataInterval(start=start, end=run_after)

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        if last_automated_data_interval is None:
            next_start = restriction.earliest
        else:
            next_start = last_automated_data_interval.end.add(days=1)
        # Skip weekends
        while next_start.day_of_week in (5, 6):
            next_start = next_start.add(days=1)
        if restriction.latest and next_start > restriction.latest:
            return None
        return DagRunInfo.interval(
            start=next_start,
            end=next_start.add(days=1),
        )

class MyPlugin(AirflowPlugin):
    name = "my_plugin"
    timetables = [BusinessDayTimetable]
```

### Logical Date vs Data Interval

```
schedule="@daily"  start_date=2024-01-01

DagRun 1: logical_date=2024-01-01, data_interval=(2024-01-01, 2024-01-02)
DagRun 2: logical_date=2024-01-02, data_interval=(2024-01-02, 2024-01-03)
```

```python
# In tasks — access dates
def my_task(ds, ds_nodash, data_interval_start, data_interval_end, **kwargs):
    print(ds)                    # "2024-01-01"
    print(ds_nodash)             # "20240101"
    print(data_interval_start)   # pendulum.DateTime
    print(data_interval_end)
```

### Jinja Template Variables

| Variable | Type | Value |
|---|---|---|
| `{{ ds }}` | str | `YYYY-MM-DD` (logical date) |
| `{{ ds_nodash }}` | str | `YYYYMMDD` |
| `{{ ts }}` | str | ISO 8601 timestamp |
| `{{ ts_nodash }}` | str | `YYYYMMDDTHHmmss` |
| `{{ run_id }}` | str | Unique DagRun ID |
| `{{ dag.dag_id }}` | str | DAG ID |
| `{{ task.task_id }}` | str | Task ID |
| `{{ ti.try_number }}` | int | Current attempt |
| `{{ data_interval_start }}` | datetime | Interval start |
| `{{ data_interval_end }}` | datetime | Interval end |
| `{{ prev_ds }}` | str | Previous ds |
| `{{ next_ds }}` | str | Next ds |
| `{{ macros.ds_add(ds, 7) }}` | str | Date arithmetic |
| `{{ var.value.KEY }}` | str | Airflow Variable |
| `{{ var.json.KEY.field }}` | any | JSON Variable field |
| `{{ conn.CONN_ID.host }}` | str | Connection host |
| `{{ params.KEY }}` | any | DAG/task param |

---

## Dynamic DAGs

### Pattern 1 — Loop over config

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

SOURCES = ["source_a", "source_b", "source_c"]

with DAG("dynamic_sources", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    for source in SOURCES:
        PythonOperator(
            task_id=f"process_{source}",
            python_callable=lambda s=source: print(f"Processing {s}"),
        )
```

### Pattern 2 — DAG Factory

```python
# dags/dag_factory.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

PIPELINE_CONFIGS = [
    {"dag_id": "pipeline_finance",  "schedule": "0 6 * * *", "table": "finance"},
    {"dag_id": "pipeline_logistics","schedule": "0 7 * * *", "table": "logistics"},
]

def make_dag(cfg: dict) -> DAG:
    with DAG(
        dag_id=cfg["dag_id"],
        start_date=datetime(2024, 1, 1),
        schedule=cfg["schedule"],
        catchup=False,
        tags=["factory"],
    ) as dag:
        PythonOperator(
            task_id="process",
            python_callable=lambda table=cfg["table"]: print(f"Table: {table}"),
        )
    return dag

for cfg in PIPELINE_CONFIGS:
    globals()[cfg["dag_id"]] = make_dag(cfg)
```

### Pattern 3 — Dynamic Task Mapping (Airflow 2.3+)

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False)
def dynamic_mapping():

    @task
    def get_items() -> list:
        return ["a", "b", "c", "d", "e"]

    @task
    def process_item(item: str) -> str:
        return f"processed_{item}"

    # expand() creates one task instance per list element
    items = get_items()
    results = process_item.expand(item=items)

    # expand_kwargs() for multiple arguments
    @task
    def load(source: str, dest: str) -> None:
        print(f"{source} -> {dest}")

    load.expand_kwargs([
        {"source": "s3://a", "dest": "db.a"},
        {"source": "s3://b", "dest": "db.b"},
    ])

dag = dynamic_mapping()
```

---

## Branching

```python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty  import EmptyOperator

def choose_branch(ds, **kwargs):
    import datetime
    day = datetime.datetime.strptime(ds, "%Y-%m-%d").weekday()
    return "weekday_task" if day < 5 else "weekend_task"

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=choose_branch,
)

weekday_task = EmptyOperator(task_id="weekday_task")
weekend_task = EmptyOperator(task_id="weekend_task")
join         = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

branch >> [weekday_task, weekend_task] >> join

# TaskFlow branch
from airflow.decorators import task

@task.branch()
def select_env() -> str:
    return "prod_load" if True else "staging_load"
```

### ShortCircuitOperator

```python
from airflow.operators.python import ShortCircuitOperator

def check_data_exists(ds, **kwargs) -> bool:
    return True  # False = skip all downstream

gate = ShortCircuitOperator(
    task_id="gate",
    python_callable=check_data_exists,
    ignore_downstream_trigger_rules=True,  # propagate skip
)
```

---

## Dependencies

```python
# Bitshift syntax (recommended)
a >> b >> c          # a → b → c
a >> [b, c] >> d     # fan-out / fan-in
[a, b] >> c          # both must complete before c

# Explicit method
a.set_downstream(b)
b.set_upstream(a)

# Chain utility
from airflow.models.baseoperator import chain, cross_downstream

chain(a, b, c)                    # a >> b >> c
chain(a, [b, c], [d, e], f)      # a >> b,c; b>>d, c>>e; d,e >> f
cross_downstream([a, b], [c, d]) # a,b >> c and a,b >> d
```

---

## Datasets (Data-Aware Scheduling — Airflow 2.4+)

```python
from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime

# Define datasets
orders_dataset   = Dataset("s3://bucket/orders/")
summary_dataset  = Dataset("s3://bucket/summary/")

# Producer DAG — updates a dataset
@dag(start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False)
def producer():
    @task(outlets=[orders_dataset])   # marks dataset as updated
    def produce():
        return "wrote orders"
    produce()

# Consumer DAG — triggered when dataset is updated
@dag(start_date=datetime(2024, 1, 1), schedule=[orders_dataset], catchup=False)
def consumer():
    @task(inlets=[orders_dataset], outlets=[summary_dataset])
    def consume():
        return "consumed orders, produced summary"
    consume()

producer_dag = producer()
consumer_dag = consumer()
```

---

## Callbacks

```python
from airflow.utils.context import Context

def on_failure(context: Context):
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]
    exc     = context.get("exception")
    # Send Slack/PagerDuty alert
    print(f"FAILED: {dag_id}.{task_id} — {exc}")

def on_success(context: Context):
    print(f"SUCCESS: {context['dag'].dag_id}")

def on_retry(context: Context):
    ti = context["ti"]
    print(f"RETRY #{ti.try_number}: {ti.task_id}")

def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA missed for DAG {dag.dag_id}")

with DAG(
    "my_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    on_failure_callback=on_failure,
    sla_miss_callback=on_sla_miss,
) as dag:
    t = PythonOperator(
        task_id="t",
        python_callable=lambda: None,
        on_failure_callback=on_failure,
        on_success_callback=on_success,
        on_retry_callback=on_retry,
        sla=timedelta(hours=1),
    )
```

---

## Pools & Priorities

```python
# Create pool via CLI:
# airflow pools set db_pool 5 "Database connection pool"

# Assign task to pool
t = PythonOperator(
    task_id="db_query",
    python_callable=my_func,
    pool="db_pool",
    pool_slots=2,         # consumes 2 of 5 slots
    priority_weight=10,   # higher = scheduled first
    weight_rule="absolute",  # "downstream" | "upstream" | "absolute"
)
```

---

## Custom Operators & Hooks

```python
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base          import BaseHook

class MyApiHook(BaseHook):
    conn_name_attr = "my_api_conn_id"
    default_conn_name = "my_api_default"
    conn_type = "http"
    hook_name = "My API"

    def __init__(self, my_api_conn_id: str = default_conn_name):
        super().__init__()
        self.my_api_conn_id = my_api_conn_id
        self.conn = None

    def get_conn(self):
        if self.conn is None:
            c = self.get_connection(self.my_api_conn_id)
            import requests
            session = requests.Session()
            session.headers.update({"Authorization": f"Bearer {c.password}"})
            session.base_url = f"https://{c.host}"
            self.conn = session
        return self.conn

    def get_data(self, endpoint: str) -> dict:
        r = self.get_conn().get(endpoint)
        r.raise_for_status()
        return r.json()


class MyApiOperator(BaseOperator):
    template_fields = ("endpoint", "date")  # Jinja-rendered fields

    def __init__(self, endpoint: str, date: str, conn_id: str = "my_api_default", **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.date = date
        self.conn_id = conn_id

    def execute(self, context):
        hook = MyApiHook(my_api_conn_id=self.conn_id)
        result = hook.get_data(f"{self.endpoint}?date={self.date}")
        self.log.info("Got %d records", len(result))
        return result
```

---

## Setup & Configuration

### Installation

```bash
# Constrained install (match Python and Airflow versions)
AIRFLOW_VERSION=2.9.0
PYTHON_VERSION=$(python --version | cut -d" " -f2 | cut -d"." -f1-2)
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "${CONSTRAINT_URL}"

# With extras
pip install "apache-airflow[postgres,celery,redis,databricks,amazon,microsoft.azure]==${AIRFLOW_VERSION}" \
  --constraint "${CONSTRAINT_URL}"
```

### Environment Variables

```bash
export AIRFLOW_HOME=/opt/airflow
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@postgres:5432/airflow
export AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
export AIRFLOW__WEBSERVER__SECRET_KEY=mysecretkey
export AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
export AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://user:pass@postgres:5432/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=UTC
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
```

### airflow.cfg Key Sections

```ini
[core]
executor = CeleryExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow
fernet_key = <generated>
dags_folder = /opt/airflow/dags
load_examples = False
parallelism = 32               # max tasks running globally
max_active_tasks_per_dag = 16
dag_file_processor_timeout = 50

[scheduler]
dag_dir_list_interval = 30
min_file_process_interval = 30
max_dagruns_to_create_per_loop = 10

[webserver]
base_url = http://airflow.example.com
secret_key = <random>
workers = 4
expose_config = False

[celery]
broker_url = redis://redis:6379/0
result_backend = db+postgresql://airflow:airflow@postgres/airflow
worker_concurrency = 16

[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend

[smtp]
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_starttls = True
smtp_user = airflow@example.com
smtp_password = <password>
smtp_mail_from = airflow@example.com
```

### Docker Compose (quick start)

```yaml
version: "3.8"
x-airflow-common: &airflow-common
  image: apache/airflow:2.9.0
  environment:
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ""
    AIRFLOW__CORE__LOAD_EXAMPLES: "False"
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"

services:
  postgres:
    image: postgres:15
    environment: {POSTGRES_USER: airflow, POSTGRES_PASSWORD: airflow, POSTGRES_DB: airflow}
    volumes: [postgres-db:/var/lib/postgresql/data]

  redis:
    image: redis:7

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports: ["8080:8080"]

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  airflow-worker:
    <<: *airflow-common
    command: celery worker

  airflow-triggerer:    # required for deferrable operators
    <<: *airflow-common
    command: triggerer

  airflow-init:
    <<: *airflow-common
    command: version
    entrypoint: /bin/bash -c "airflow db migrate && airflow users create --username admin --password admin --firstname A --lastname B --role Admin --email admin@example.com"

volumes:
  postgres-db:
```

---

## CLI Reference

```bash
# Database
airflow db migrate              # Apply schema migrations
airflow db init                 # Initialize (first time)
airflow db reset                # !! Wipe everything
airflow db check                # Test connection

# Users
airflow users create --username admin --password admin \
  --firstname A --lastname B --role Admin --email admin@example.com
airflow users list
airflow users delete --username admin

# DAGs
airflow dags list
airflow dags list-import-errors
airflow dags trigger my_dag --conf '{"key": "value"}'
airflow dags pause   my_dag
airflow dags unpause my_dag
airflow dags backfill my_dag -s 2024-01-01 -e 2024-01-31
airflow dags delete  my_dag
airflow dags show    my_dag   # ASCII graph

# Tasks
airflow tasks list my_dag
airflow tasks test my_dag my_task 2024-01-01   # local test, no DB state
airflow tasks run  my_dag my_task run_id        # run a specific task
airflow tasks clear my_dag -t my_task -s 2024-01-01 -e 2024-01-31
airflow tasks states-for-dag-run my_dag run_id

# Connections
airflow connections list
airflow connections add my_conn --conn-type postgres --conn-host localhost \
  --conn-login user --conn-password secret --conn-port 5432 --conn-schema mydb
airflow connections delete my_conn
airflow connections export connections.json

# Variables
airflow variables set key value
airflow variables get key
airflow variables delete key
airflow variables export vars.json
airflow variables import vars.json

# Pools
airflow pools list
airflow pools set my_pool 10 "description"
airflow pools delete my_pool

# Logs & Monitoring
airflow scheduler -D             # run as daemon
airflow celery worker -D
airflow celery flower             # Celery monitoring UI on :5555
airflow version
```

---

## Best Practices

### DAG Design

```python
# ✅ Always set catchup=False unless you need backfill
with DAG(..., catchup=False):

# ✅ Use start_date in the past (never "now")
start_date=datetime(2024, 1, 1)  # fixed date

# ✅ Set max_active_runs=1 to prevent overlapping
max_active_runs=1

# ✅ Use Tags for filtering in UI
tags=["etl", "finance", "daily"]

# ✅ Document your DAG
__doc__ = """
## My Pipeline
Processes daily financial data from S3 → Postgres.
**Owner:** Data Engineering
**SLA:** 8:00 AM UTC
"""

# ❌ Never put heavy logic at DAG parse time (top-level code)
# This runs every scheduler loop!
records = db.query("SELECT COUNT(*) FROM table")  # BAD — do inside task

# ✅ Keep DAG definition lightweight
```

### Task Design

```python
# ✅ Tasks must be idempotent
def idempotent_load(ds, **kwargs):
    # Delete + reinsert, not just insert
    conn.execute(f"DELETE FROM table WHERE date = '{ds}'")
    conn.execute(f"INSERT INTO table SELECT ... WHERE date = '{ds}'")

# ✅ Use sensors in reschedule mode
wait = FileSensor(mode="reschedule", ...)  # don't block worker slots

# ✅ Use pools to limit concurrency on shared resources
PythonOperator(pool="db_pool", pool_slots=1, ...)

# ✅ Use retries + exponential backoff for flaky sources
PythonOperator(retries=3, retry_delay=timedelta(minutes=5),
               retry_exponential_backoff=True)

# ✅ Avoid passing large data via XCom — use external storage
def process(ti, **kwargs):
    # BAD:  ti.xcom_push("data", huge_dataframe.to_dict())
    # GOOD: write to S3/GCS/ADLS, push only the path
    path = "s3://bucket/tmp/result.parquet"
    df.to_parquet(path)
    ti.xcom_push(key="result_path", value=path)
```

### Security

```python
# ✅ Use Secrets Backend for credentials (not plaintext Variables/Connections)
# ✅ Use Fernet encryption for connections in metadata DB
# ✅ Rotate Fernet key when rotating credentials:
#    airflow rotate-fernet-key
# ✅ Disable expose_config in webserver for production
# ✅ Use RBAC (Role-Based Access Control) — enabled by default in 2.x
# ✅ Never hardcode credentials in DAG files
```

### Performance

```bash
# Tune parallelism for your setup
AIRFLOW__CORE__PARALLELISM=64          # global task slots
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP=10

# Use LocalExecutor for single-node setups (simpler than Celery)
# Use CeleryExecutor for multi-worker horizontal scaling
# Use KubernetesExecutor for dynamic pod-per-task scaling

# ✅ Use deferrable operators to reduce worker slot consumption
# ✅ Use dynamic task mapping instead of large fan-out DAGs
```

---

## Design Patterns

### Pattern: ELT Pipeline

```python
@dag(start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False)
def elt_pipeline():

    @task
    def extract(ds) -> str:
        # Extract from source, write to landing zone
        path = f"s3://bucket/landing/{ds}/data.parquet"
        # ... extraction logic ...
        return path

    @task
    def load_raw(path: str, ds) -> None:
        hook = PostgresHook(postgres_conn_id="dw")
        hook.run(f"COPY raw.table FROM STDIN ...", ...)

    @task
    def transform(ds) -> None:
        hook = PostgresHook(postgres_conn_id="dw")
        hook.run(f"INSERT INTO curated.table SELECT ... FROM raw.table WHERE date='{ds}'")

    path = extract()
    load_raw(path) >> transform()

dag = elt_pipeline()
```

### Pattern: Sensor → Process → Notify

```python
with DAG("sensor_pattern", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:

    wait  = FileSensor(task_id="wait", filepath="/data/{{ ds }}.csv", mode="reschedule")
    proc  = PythonOperator(task_id="process", python_callable=process_file)
    ok    = EmailOperator(task_id="notify_ok",  to=["team@co.com"], subject="Done {{ ds }}", html_content="OK")
    fail  = EmailOperator(task_id="notify_fail", to=["team@co.com"], subject="FAILED {{ ds }}", html_content="FAIL",
                          trigger_rule="one_failed")

    wait >> proc >> [ok, fail]
```

### Pattern: Parallel Processing with Join

```python
with DAG("parallel_join", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule="all_success")

    tasks = [
        PythonOperator(task_id=f"process_{region}", python_callable=process, op_kwargs={"region": region})
        for region in ["eu", "us", "apac"]
    ]

    start >> tasks >> end
```

### Pattern: Conditional Backfill Gate

```python
@task.branch()
def check_backfill(ds, **kwargs) -> str:
    from datetime import datetime
    return "backfill_task" if datetime.strptime(ds, "%Y-%m-%d") < datetime(2024, 6, 1) else "normal_task"
```

### Pattern: Dynamic DAG per Table (Factory)

```python
TABLES = ["orders", "users", "products"]

def make_table_dag(table: str):
    with DAG(f"sync_{table}", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
        extract = BashOperator(task_id="extract", bash_command=f"python extract.py --table {table}")
        load    = BashOperator(task_id="load",    bash_command=f"python load.py --table {table}")
        extract >> load
    return dag

for t in TABLES:
    globals()[f"sync_{t}"] = make_table_dag(t)
```

### Pattern: Cross-DAG Dependency

```python
# Child DAG waits for parent
with DAG("child", start_date=datetime(2024, 1, 1), schedule="@daily") as dag:
    wait = ExternalTaskSensor(
        task_id="wait_parent",
        external_dag_id="parent",
        external_task_id="final",
        execution_delta=timedelta(0),
        mode="reschedule",
        timeout=3600,
    )
    process = PythonOperator(task_id="process", python_callable=my_fn)
    wait >> process
```

### Pattern: SLA Monitoring with Alerting

```python
from airflow.utils.email import send_email

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    send_email(
        to=["oncall@example.com"],
        subject=f"SLA Miss: {dag.dag_id}",
        html_content=f"Tasks: {[t.task_id for t in task_list]}",
    )

with DAG("monitored", sla_miss_callback=sla_miss_callback, ...):
    critical_task = PythonOperator(
        task_id="critical",
        python_callable=my_fn,
        sla=timedelta(hours=2),
    )
```

---

## Python Integration Patterns

### Python Callable Best Practices

```python
# ✅ Accept **kwargs to get the full context dict
def my_task(ds, run_id, ti, dag, task, **kwargs):
    # ds          = "2024-01-01"
    # run_id      = "scheduled__2024-01-01T00:00:00+00:00"
    # ti          = TaskInstance object
    # dag         = DAG object
    # task        = BaseOperator object
    pass

# ✅ Use type hints with TaskFlow
@task
def typed_task(records: list[dict], config: dict) -> dict:
    return {"processed": len(records)}

# ✅ Separate business logic from Airflow glue
# my_lib/processor.py — pure Python, no Airflow imports
def process_data(records: list, date: str) -> dict:
    return {"count": len(records), "date": date}

# dags/my_dag.py — thin Airflow wrapper
from my_lib.processor import process_data

@task
def run_processor(ds) -> dict:
    records = fetch_records(ds)
    return process_data(records, ds)
```

### Using pandas / PySpark in Tasks

```python
@task.virtualenv(requirements=["pandas", "pyarrow"])
def pandas_transform(ds: str) -> str:
    import pandas as pd
    df = pd.read_parquet(f"s3://bucket/raw/{ds}.parquet")
    df["processed"] = True
    out = f"s3://bucket/curated/{ds}.parquet"
    df.to_parquet(out)
    return out

# Databricks / PySpark — submit via DatabricksSubmitRunOperator
# (don't run Spark in Airflow workers — submit jobs remotely)
```

### Airflow + dbt

```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="dbt run --profiles-dir /opt/airflow/dbt --target prod --select tag:daily",
    env={"DBT_PROFILES_DIR": "/opt/airflow/dbt"},
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="dbt test --profiles-dir /opt/airflow/dbt --select tag:daily",
)

dbt_run >> dbt_test
```

### Airflow + Great Expectations

```python
from airflow.operators.python import PythonOperator

def run_ge_checkpoint(checkpoint_name: str, ds: str, **kwargs):
    import great_expectations as ge
    context = ge.get_context()
    result  = context.run_checkpoint(checkpoint_name=checkpoint_name, batch_request={"runtime_parameters": {"query": f"SELECT * FROM table WHERE date='{ds}'"}})
    if not result.success:
        raise ValueError(f"Data quality check failed: {result}")

validate = PythonOperator(
    task_id="validate",
    python_callable=run_ge_checkpoint,
    op_kwargs={"checkpoint_name": "orders_checkpoint"},
)
```

---

## Testing

```python
# Unit test — no Airflow running
from airflow.models import DagBag

def test_dag_loads():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "my_dag" in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_task_count():
    dagbag = DagBag()
    dag = dagbag.get_dag("my_dag")
    assert len(dag.tasks) == 5

def test_task_dependencies():
    dag = DagBag().get_dag("my_dag")
    task_a = dag.get_task("task_a")
    task_b = dag.get_task("task_b")
    assert task_b in task_a.downstream_list

# Test task logic independently
from unittest.mock import MagicMock, patch

def test_process_logic():
    from dags.my_dag import process_data
    result = process_data(records=[1, 2, 3], ds="2024-01-01")
    assert result["count"] == 3

# CLI-based test (no DB state written)
# airflow tasks test my_dag my_task 2024-01-01
```

---

## Monitoring & Observability

```python
# StatsD metrics (enable in airflow.cfg)
# [metrics]
# statsd_on = True
# statsd_host = localhost
# statsd_port = 8125
# statsd_prefix = airflow

# Key metrics to monitor:
# airflow.dag_processing.total_parse_time
# airflow.executor.open_slots
# airflow.executor.queued_tasks
# airflow.executor.running_tasks
# airflow.ti.failures
# airflow.ti.successes
# airflow.dagrun.duration.success.<dag_id>
# airflow.dagrun.duration.failed.<dag_id>
# airflow.scheduler.tasks.killed_externally

# OpenTelemetry (Airflow 2.7+)
# [metrics]
# otel_on = True
# otel_host = localhost
# otel_port = 4318
```

---

## Quick Reference: Executor Comparison

| Executor | Workers | Use case | Notes |
|---|---|---|---|
| `SequentialExecutor` | 1 | Dev / SQLite only | Default; not for prod |
| `LocalExecutor` | N (subprocess) | Single node prod | Simple, no broker needed |
| `CeleryExecutor` | N (distributed) | Multi-node prod | Needs Redis/RabbitMQ |
| `KubernetesExecutor` | 1 pod/task | Cloud-native | Best isolation, slow start |
| `CeleryKubernetesExecutor` | Both | Hybrid | Celery + K8s queues |
| `LocalKubernetesExecutor` | Both | Dev + prod hybrid | New in 2.3 |

---

## Quick Reference: Provider Packages

| Provider | pip extra | Key operators |
|---|---|---|
| PostgreSQL | `postgres` | `PostgresOperator`, `PostgresHook` |
| MySQL | `mysql` | `MySqlOperator`, `MySqlHook` |
| Amazon AWS | `amazon` | `S3KeySensor`, `GlueOperator`, `RedshiftOperator` |
| Azure | `microsoft.azure` | `AzureDataFactoryRunPipelineOperator`, `WasbSensor` |
| GCP | `google` | `BigQueryOperator`, `GCSObjectExistenceSensor` |
| Databricks | `databricks` | `DatabricksRunNowOperator`, `DatabricksSubmitRunOperator` |
| dbt | `dbt-cloud` | `DbtCloudRunJobOperator` |
| Snowflake | `snowflake` | `SnowflakeOperator`, `SnowflakeHook` |
| Spark | `apache.spark` | `SparkSubmitOperator`, `SparkJDBCOperator` |
| HTTP | `http` | `SimpleHttpOperator`, `HttpSensor`, `HttpHook` |
| Slack | `slack` | `SlackAPIPostOperator`, `SlackWebhookOperator` |
| PagerDuty | `pagerduty` | `PagerdutyEventsOperator` |
| SSH | `ssh` | `SSHOperator`, `SFTPOperator`, `SSHHook` |
| Docker | `docker` | `DockerOperator` |
| Kubernetes | `cncf.kubernetes` | `KubernetesPodOperator` |

---

*Generated for Apache Airflow 2.x — verify provider package versions against your Airflow release.*
