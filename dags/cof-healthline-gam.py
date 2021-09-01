from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 13),
    'email': ['ckonatalapalli@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-lake-dw-cdm-sdk-cof-healthline-gam'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s/%s' % (ACCOUNT, Variable.get("environment"), DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
large_task_custom_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'num_workers': Variable.get("DBX_LARGE_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'cluster_log_conf': LOG_PATH,
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.driver.extraJavaOptions': '-Dconfig.resource=' + Variable.get("COF_SDK_CONFIG_FILE"),
        'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
        'java_opts': '-Dconfig.resource=' + Variable.get("COF_SDK_CONFIG_FILE")
    },
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        'ebs_volume_count': 1,
        'ebs_volume_size': 400,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '0',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}
gam_mapping_large_task_custom_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'num_workers': Variable.get("DBX_LARGE_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'cluster_log_conf': LOG_PATH,
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.driver.extraJavaOptions': '-Dconfig.resource=' + Variable.get("SDK_CONFIG_FILE"),
        'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
        'java_opts': '-Dconfig.resource=' + Variable.get("SDK_CONFIG_FILE")
    },
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        'ebs_volume_count': 1,
        'ebs_volume_size': 400,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '0',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

# Libraries
cof_staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/FileStore/jars/d69470f3_6b85_4b6a_9f84_677fed6a9631-cdm_cof_assembly_1_0_6-e0007.jar",
    },
]

staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/" + Variable.get("environment") + "/scala-2.12/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
    },
]

gam_jar_task = {
    'main_class_name': "com.redventures.cdm.cof.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cof.staging.GamByOrderIds",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("HL_GAM_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

cof_report_hl_mapping_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.HLGamMapping",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("HL_GAM_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__crosssitelookback=" + Variable.get("DBX_gam_crosssite_lookback_days")
    ]
}

# DAG Creation Step
with DAG(DAG_NAME,
         schedule_interval='0 9 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    gam_report_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-healthline-gam',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=gam_jar_task,
        libraries=cof_staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    hl_gam_mapping_report_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-healthline-gam-mapping',
        new_cluster=gam_mapping_large_task_custom_cluster,
        spark_jar_task=cof_report_hl_mapping_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# dependencies
gam_report_task >> hl_gam_mapping_report_task
