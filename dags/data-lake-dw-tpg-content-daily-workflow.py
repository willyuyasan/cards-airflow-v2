# Migrating Legacy to New Airflow 6/7/2021
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import BranchPythonOperator
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 1),
    'email': ['vmalhotra@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"

# Cluster Setup Step


small_task_cluster = {
    'spark_version': '5.3.x-scala2.11',
    'node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'num_workers': Variable.get("DBX_SMALL_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf': 'dbfs://home/cluster_log',
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'spark_env_vars': {
        'API_SECRET_SCOPE': 'cards', 'MANUAL_JOB_ID': 'cards', 'CDM_SECRET_SCOPE': 'cards'
    },
    'aws_attributes': {
        'ebs_volume_count': 1,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        'availability': 'SPOT_WITH_FALLBACK',
        'instance_profile_arn': Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': ' B532',
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}


# Libraries
notebook_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/data-common-jars/production/data_common_2_0_0.jar",
    },
    {
        "jar": "dbfs:/FileStore/jars/8a230257_57b0_4170_9d58_57ab8b160f78-scalapb_models_assembly_bom_1_326_0-c0937.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/CDM-Ignite/CDM-Ignite-assembly-1.0.2-SNAPSHOT.jar",
    },
]

# Notebook Task Parameter Setup:
content_roi_notebook_task = {
    'base_parameters': {
        "writeBucket": Variable.get("DBX_TPG_Content_writeBucket"),
        "daysToWrite": Variable.get("DBX_TPG_Content_daysToWrite"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Audience_Analytics_redshiftEnvironment"),
        "maxAnonSize": "250"
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/ContentROI/TPGContentROI',
}

# DAG Creation Step
with DAG('data-lake-dw-tpg-content-daily-workflow',
         schedule_interval='30 5 * * *',
         dagrun_timeout=timedelta(hours=6),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    # Content ROI Workflow

    content_roi = FinServDatabricksSubmitRunOperator(
        task_id='content-roi',
        new_cluster=small_task_cluster,
        notebook_task=content_roi_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3000,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
