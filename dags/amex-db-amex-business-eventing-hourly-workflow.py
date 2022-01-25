# Migrating Legacy to New Airflow 6/7/2021
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from operators.finserv_cdm_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 19),
    'email': ['vmalhotra@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-travel-name")),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")

}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"

base_params = {
    "lookBackDays": Variable.get("DBX_AMEX_BUSINESS_EVENTING_LOOKBACK_DAYS"),
    "lookbackHours": Variable.get("DBX_AMEX_BUSINESS_EVENTING_LOOKBACK_HOURS"),
    "loggingPath": Variable.get("DBX_AMEX_BUSINESS_Logging_Path"),
    "dataLakePath": Variable.get("DBX_DataLake_Path"),
    "amexBusinessOpenWriteKey": Variable.get("DBX_AMEX_BUSINESS_OPEN_WRITE_KEY"),
    "amexBusinessWriteKey": Variable.get("DBX_AMEX_BUSINESS_WRITE_KEY"),
    "companyIdName": "amex",
    "tenantID": Variable.get("DBX_AMEX_BUSINESS_Tenant_Id"),
    "airflowDAGRunID": "{{ ti.dag_id }}" + "-" + "{{ ti.task_id }}" + "-" + "{{ ti.execution_date }}",
    "LoadType": Variable.get("DBX_AMEX_BUSINESS_EVENTING_LOAD_TYPE"),
    "toDate": "now",
    "amexBusinessLogTable": Variable.get("DBX_AMEX_BUSINESS_LOG_TABLE")

}

# Cluster Setup Step
small_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'num_workers': Variable.get("DBX_SMALL_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf': 'dbfs://home/cluster_log',
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'aws_attributes': {
        'availability': 'SPOT_WITH_FALLBACK',
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        'instance_profile_arn': Variable.get("DBX_AMEX_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B130',
        'Project': 'American Express - OPEN'
    },
}

# Libraries
staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/data-common/data-common-3.0.1-2.jar",

    },
    {
        "maven": {
            "coordinates": "org.scalaj:scalaj-http_2.12:2.4.2"
        }
    }
]

# Notebook Task Parameter Setup:
i3_call_data_notebook_task = {
    'base_parameters': {
        "sendEventProcess": "I3"
    },
    'notebook_path': '/Production/cards-data-mart-amex-business/' + Variable.get("DBX_AMEX_BUSINESS_CODE_ENV") + '/eventing-notebooks/i3CallData',
}

ivr_path_data_notebook_task = {
    'base_parameters': {
        "sendEventProcess": "IVRPathCall"
    },
    'notebook_path': '/Production/cards-data-mart-amex-business/' + Variable.get("DBX_AMEX_BUSINESS_CODE_ENV") + '/eventing-notebooks/ivrPathData',
}

delta_optimization_notebook_task = {
    'base_parameters': {
        "tableName": Variable.get("DBX_AMEX_BUSINESS_LOG_TABLE")
    },
    'notebook_path': '/Production/cards-data-mart-amex-business/' + Variable.get("DBX_AMEX_BUSINESS_CODE_ENV") + '/eventing-notebooks/optimizeDeltaTablesAMEXBusiness',
}

# updating base params
i3_call_data_notebook_task['base_parameters'].update(base_params)
ivr_path_data_notebook_task['base_parameters'].update(base_params)

# DAG Creation Step
with DAG('amex-db-amex-business-eventing-hourly-workflow',
         schedule_interval='30 0-4,7-23 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    i3_call_data_task = FinServDatabricksSubmitRunOperator(
        task_id='i3-call-data',
        new_cluster=small_task_cluster,
        notebook_task=i3_call_data_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=900,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    ivr_path_call_data_task = FinServDatabricksSubmitRunOperator(
        task_id='ivr-path-call-data',
        new_cluster=small_task_cluster,
        notebook_task=ivr_path_data_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=900,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    delta_optimization_task = FinServDatabricksSubmitRunOperator(
        task_id='delta-table-optimization',
        new_cluster=small_task_cluster,
        notebook_task=delta_optimization_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=900,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# Defining  dependencies
i3_call_data_task >> delta_optimization_task
ivr_path_call_data_task >> delta_optimization_task
