from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators import ExternalTaskSensor
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 21),
    'email': ['kbhargavaram@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'op_kwargs': cfg_dict,
    'provide_context': True,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-lake-dw-cdm-sdk-tpg-app-reporting-hourly'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s/%s' % (ACCOUNT, Variable.get("environment"), DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
small_task_cluster = {
    'spark_version': '5.3.x-scala2.11',
    'node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'num_workers': Variable.get("DBX_SMALL_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'cluster_log_conf': LOG_PATH,
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'aws_attributes': {
        'ebs_volume_count': 2,
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
staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/data-warehouse/production/datawarehouse-builder-0.5.4.jar",
    },
]

reporting_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/" + Variable.get("environment") + "/scala-2.12/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
    }
]

# Dimension tables task
dimension_tables_notebook_task = {
    'base_parameters': {},
    'notebook_path': '/Production/cards-data-mart-tpg/' + Variable.get("DBX_TPG_CODE_ENV") + '/staging-table-notebooks/stg_DimensionTables',
}

# Reporting table tasks
screenview_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("TPG_SHORT_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.MobileScreenView",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

form_summary_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("TPG_SHORT_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.MobileFormSummary",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

waitlist_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("TPG_SHORT_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_TPG_WEB_AND_APP_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.Waitlist",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

# DAG Creation Step
with DAG('data-lake-dw-cdm-sdk-tpg-app-reporting-hourly',
         schedule_interval='0 0-6,11-23 * * *',
         dagrun_timeout=timedelta(hours=4),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    tpg_app_staging_tables = ExternalTaskSensor(
        task_id='external-tpg-app-reporting',
        external_dag_id='data-lake-dw-cdm-sdk-cards-staging-hourly',
        external_task_id='external-tpg-app-staging',
        execution_timeout=timedelta(minutes=15),
        execution_delta=timedelta(minutes=30)
    )

    screenview_reporting = FinServDatabricksSubmitRunOperator(
        task_id='screenview-reporting',
        new_cluster=small_task_cluster,
        spark_jar_task=screenview_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    form_summary_reporting = FinServDatabricksSubmitRunOperator(
        task_id='form-summary-reporting',
        new_cluster=small_task_cluster,
        spark_jar_task=form_summary_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    waitlist_reporting = FinServDatabricksSubmitRunOperator(
        task_id='waitlist-reporting',
        new_cluster=small_task_cluster,
        spark_jar_task=waitlist_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

# Dependencies
tpg_app_staging_tables >> [screenview_reporting, form_summary_reporting, waitlist_reporting]
