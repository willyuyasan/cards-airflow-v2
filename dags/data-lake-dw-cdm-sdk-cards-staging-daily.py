from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email': ['vmalhotra@redventures.com'],
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
DAG_NAME = 'data-lake-dw-cdm-sdk-cards-staging-daily'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s/%s' % (
            ACCOUNT, Variable.get("environment"), DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
extra_small_task_custom_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'm5a.xlarge',
    'driver_node_type_id': 'm5a.xlarge',
    'num_workers': 4,
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
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1b',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B814',
        'Project': 'Cards Allocation',
        'Dag_id': "{{ ti.dag_id }}",
        'Task_id': "{{ ti.task_id }}"
    },
}

small_task_custom_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'm5a.xlarge',
    'driver_node_type_id': 'm5a.xlarge',
    'num_workers': 4,
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
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1b',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B814',
        'Project': 'Cards Allocation',
        'Dag_id': "{{ ti.dag_id }}",
        'Task_id': "{{ ti.task_id }}"
    },
}

medium_task_custom_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'm5a.2xlarge',
    'driver_node_type_id': 'm5a.2xlarge',
    'num_workers': 6,
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
        'ebs_volume_count': 3,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1b',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B814',
        'Project': 'Cards Allocation',
        'Dag_id': "{{ ti.dag_id }}",
        'Task_id': "{{ ti.task_id }}"
    },
}

large_task_custom_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'm5a.4xlarge',
    'driver_node_type_id': 'm5a.4xlarge',
    'num_workers': 8,
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
        'ebs_volume_count': 3,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1b',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B814',
        'Project': 'Cards Allocation',
        'Dag_id': "{{ ti.dag_id }}",
        'Task_id': "{{ ti.task_id }}"
    },
}

# Libraries
staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/" + Variable.get(
            "environment") + "/scala-2.12/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
    },
]

# Notebook Task Parameter Setup:
session_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.Session",
        "lookBackDays=" + "10",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

page_view_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.PageView",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

page_view_staging_cof_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=2))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.PageView",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_COF_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

cookie_identified_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.CookieIdentified",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

field_inputted_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FieldsInputted",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

field_selected_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FieldSelected",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

field_selected_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FieldSelected",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
location_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.Location",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

device_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.Device",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

decision_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.Decision",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

traffic_sources_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.TrafficSources",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

page_metrics_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.PageMetrics",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

form_started_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FormStarted",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
form_continue_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FormContinued",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

form_submitted_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FormSubmitted",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_AMEX_BUSINESS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

element_viewed_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ElementViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

element_clicked_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ElementClicked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

product_clicked_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ProductClicked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

product_viewed_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ProductViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

amp_page_viewed_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.AmpPageViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

paidsearch_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.PaidSearch",
        "ACCOUNT=" + "cards",
        "PAID_SEARCH_COMPANY_ID=" + Variable.get("CARDS_PAIDSEARCH_COMPANY_IDS"),
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

hoppageviewed_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.HopPageViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

tpg_ccdc_ot_summary_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_TPG_CCDC_OT_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.TpgCcdcOutcomeTrackedSummary",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]

}

form_outcome_received_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_TPG_CCDC_OT_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FormOutcomeReceived",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]

}

cookies_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.Cookies",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_BUSINESS_CONSUMER_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

content_meta_data_tracked_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ContentMetadataTracked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}


# TPG APP specific
mobile_element_clicked_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileElementClicked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_form_backed_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileFormBacked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_form_continued_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileFormContinued",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_form_errored_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileFormErrored",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

mobile_form_exited_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileFormExited",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_form_outcome_received_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileFormOutcomeReceived",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_form_started_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileFormStarted",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_form_submitted_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileFormSubmitted",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_product_clicked_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileProductClicked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_product_viewed_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileProductViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_screen_engaged_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileScreenEngaged",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_screen_refreshed_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileScreenRefreshed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
mobile_screen_viewed_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileScreenViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

mobile_application_backgrounded_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileApplicationBackgrounded",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

mobile_lpupdated_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileLpUpdated",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

mobile_MXresponse_captured_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_TPG_APP_Staging_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.staging.MobileMXResponseCaptured",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_APP_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}
# AMEX specific jar:
pqo_offer_received_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.amex_consumer.staging.PQOOfferReceived",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_CONSUMER_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

pqo_offer_requested_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.amex_consumer.staging.PQOOfferRequested",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_CONSUMER_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

pzn_offers_received_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.PZNOffersReceived",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_BUSINESS_CONSUMER_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

twilio_call_connected_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.amex_business.staging.TwilioCallConnected",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("Twilio_Global_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

twilio_call_transferred_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.amex_business.staging.TwilioCallTransferred",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_BUSINESS_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

ot_details_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_AMEX_OT_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.AMEXOutcomeTrackedDetails",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_BUSINESS_CONSUMER_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__read_Data_Base=" + Variable.get("DBX_READ_DATABASE")
    ]
}

ot_summary_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_AMEX_OT_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.AMEXOutcomeTrackedSummary",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_BUSINESS_CONSUMER_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

ot_raw_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_AMEX_OT_Raw_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.OutcomeTracked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_BUSINESS_CONSUMER_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

ot_metadata_raw_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_AMEX_OT_Raw_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.OutcomeTrackedMetaData",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_AMEX_BUSINESS_CONSUMER_SDK_Tenants"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

ProductList_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_Daily_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.ccdc.staging.ProductList",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

# DAG Creation Step
with DAG('data-lake-dw-cdm-sdk-cards-staging-daily',
         schedule_interval='30 8 * * *',
         dagrun_timeout=timedelta(hours=3),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:
    session_staging = FinServDatabricksSubmitRunOperator(
        task_id='session-staging',
        new_cluster=small_task_custom_cluster,
        spark_jar_task=session_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    traffic_sources_staging = FinServDatabricksSubmitRunOperator(
        task_id='traffic-sources-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=traffic_sources_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    page_view_staging = FinServDatabricksSubmitRunOperator(
        task_id='page-view-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=page_view_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    page_view_staging_cof = FinServDatabricksSubmitRunOperator(
        task_id='page_view_staging_cof',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=page_view_staging_cof_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    cookie_identified_staging = FinServDatabricksSubmitRunOperator(
        task_id='cookie-identified-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=cookie_identified_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    ProductList_staging = FinServDatabricksSubmitRunOperator(
        task_id='productList-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=ProductList_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    field_inputted_staging = FinServDatabricksSubmitRunOperator(
        task_id='field-inputted-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=field_inputted_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    field_selected_staging = FinServDatabricksSubmitRunOperator(
            task_id='field-selected-staging',
            new_cluster=extra_small_task_custom_cluster,
            spark_jar_task=field_selected_staging_jar_task,
            libraries=staging_libraries,
            timeout_seconds=3600,
            databricks_conn_id=airflow_svc_token,
            polling_period_seconds=120
    )

    location_staging = FinServDatabricksSubmitRunOperator(
        task_id='location-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=location_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    device_staging = FinServDatabricksSubmitRunOperator(
        task_id='device-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=device_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    decsion_staging = FinServDatabricksSubmitRunOperator(
        task_id='decision-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=decision_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    page_metrics_staging = FinServDatabricksSubmitRunOperator(
        task_id='page-metrics-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=page_metrics_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
    form_submitted_staging = FinServDatabricksSubmitRunOperator(
        task_id='form-submitted-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=form_submitted_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
    form_started_staging = FinServDatabricksSubmitRunOperator(
        task_id='form-started-staging',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=form_started_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    form_continue_staging = FinServDatabricksSubmitRunOperator(
        task_id='form-continue-staging',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=form_continue_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
    form_outcome_received_staging = FinServDatabricksSubmitRunOperator(
        task_id='form-outcome-received-staging',
        new_cluster=small_task_custom_cluster,
        spark_jar_task=form_outcome_received_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_clicked_staging = FinServDatabricksSubmitRunOperator(
        task_id='product-clicked-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=product_clicked_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_viewed_staging = FinServDatabricksSubmitRunOperator(
        task_id='product-viewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=product_viewed_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    element_clicked_staging = FinServDatabricksSubmitRunOperator(
        task_id='element-clicked-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=element_clicked_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    element_viewed_staging = FinServDatabricksSubmitRunOperator(
        task_id='element-viewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=element_viewed_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    amp_page_viewed_staging = FinServDatabricksSubmitRunOperator(
        task_id='amp-page-viewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=amp_page_viewed_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    paidsearch_staging = FinServDatabricksSubmitRunOperator(
        task_id='paidsearch-staging',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=paidsearch_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    hoppageviewed_staging = FinServDatabricksSubmitRunOperator(
        task_id='hoppageviewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=hoppageviewed_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    tpg_ccdc_ot_summary_staging = FinServDatabricksSubmitRunOperator(
        task_id='tpg-ccdc-ot-summary-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=tpg_ccdc_ot_summary_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    cookies_staging = FinServDatabricksSubmitRunOperator(
        task_id='cookies-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=cookies_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    pqo_offer_received_staging = FinServDatabricksSubmitRunOperator(
        task_id='pqo-offer-received-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=pqo_offer_received_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    pqo_offer_requested_staging = FinServDatabricksSubmitRunOperator(
        task_id='pqo-offer-requested-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=pqo_offer_requested_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    pzn_offers_received_staging = FinServDatabricksSubmitRunOperator(
        task_id='pzn-offers-received-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=pzn_offers_received_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    twilio_call_connected_staging = FinServDatabricksSubmitRunOperator(
        task_id='twilio-call-connected-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=twilio_call_connected_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    twilio_call_transferred_staging = FinServDatabricksSubmitRunOperator(
        task_id='twilio-call-transferred-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=twilio_call_transferred_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    amex_ot_details_staging = FinServDatabricksSubmitRunOperator(
        task_id='amex-ot-details-staging',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=ot_details_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    amex_ot_summary_staging = FinServDatabricksSubmitRunOperator(
        task_id='amex-ot-summary-staging',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=ot_summary_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    content_meta_data_tracked_staging = FinServDatabricksSubmitRunOperator(
        task_id='content-meta-data-tracked-staging',
        new_cluster=small_task_custom_cluster,
        spark_jar_task=content_meta_data_tracked_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    ot_raw_staging = FinServDatabricksSubmitRunOperator(
        task_id='ot-raw-staging',
        new_cluster=small_task_custom_cluster,
        spark_jar_task=ot_raw_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    ot_metadata_raw_staging = FinServDatabricksSubmitRunOperator(
        task_id='ot-meta-data-raw-staging',
        new_cluster=small_task_custom_cluster,
        spark_jar_task=ot_metadata_raw_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    mobile_element_clicked_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-element-clicked-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_element_clicked_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_form_backed_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-form-backed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_form_backed_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_form_continued_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-form-continued-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_form_continued_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_form_errored_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-form-errored-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_form_errored_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_form_exited_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-form-exited-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_form_exited_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_form_outcome_received_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-form-outcome-received-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_form_outcome_received_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_form_started_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-form-started-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_form_started_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_form_submitted_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-form-submitted-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_form_submitted_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_product_clicked_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-product-clicked-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_product_clicked_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_product_viewed_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-product-viewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_product_viewed_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_screen_engaged_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-screen-engaged-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_screen_engaged_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_screen_refreshed_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-screen-refreshed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_screen_refreshed_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_screen_viewed_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-screen-viewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_screen_viewed_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    mobile_application_backgrounded = FinServDatabricksSubmitRunOperator(
        task_id='mobile-application-backgrounded-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_application_backgrounded_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_lpupdated_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-lpupdated-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_lpupdated_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )
    mobile_MXresponse_captured_staging = FinServDatabricksSubmitRunOperator(
        task_id='mobile-mxresponse-captured-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=mobile_MXresponse_captured_jar_task,
        libraries=staging_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    ccdc_staging_tables = DummyOperator(
        task_id='external-ccdc-staging'
    )

    tpg_staging_tables = DummyOperator(
        task_id='external-tpg-staging'
    )

    tpg_app_staging_tables = DummyOperator(
        task_id='external-tpg-app-staging'
    )
    amex_business_staging_tables = DummyOperator(
        task_id='external-amex-business-staging'
    )

    amex_consumer_staging_tables = DummyOperator(
        task_id='external-amex-consumer-staging'
    )

    lp_staging_tables = DummyOperator(
        task_id='external-lp-staging'
    )

    cof_staging_tables = DummyOperator(
        task_id='external-cof-staging'
    )

# Staging Dependencies
session_staging >> traffic_sources_staging
session_staging >> paidsearch_staging
paidsearch_staging >> traffic_sources_staging

# CCDC Staging Dependencies
[page_view_staging, page_metrics_staging, product_clicked_staging, product_viewed_staging, element_clicked_staging,
 element_viewed_staging, cookie_identified_staging, field_inputted_staging, field_selected_staging, device_staging, location_staging,
 decsion_staging, traffic_sources_staging, form_started_staging, form_continue_staging, form_submitted_staging, paidsearch_staging, hoppageviewed_staging,
 tpg_ccdc_ot_summary_staging, ProductList_staging] >> ccdc_staging_tables

# TPG Staging Dependencies
[page_view_staging, page_metrics_staging, product_clicked_staging, product_viewed_staging, element_clicked_staging,
 element_viewed_staging, cookie_identified_staging, field_inputted_staging, device_staging, location_staging,
 decsion_staging, traffic_sources_staging, form_submitted_staging, amp_page_viewed_staging, paidsearch_staging,
 hoppageviewed_staging, tpg_ccdc_ot_summary_staging, content_meta_data_tracked_staging] >> tpg_staging_tables

# TPG App Staging Dependencies
[mobile_element_clicked_staging, mobile_form_backed_staging, mobile_form_continued_staging, mobile_form_errored_staging,
 mobile_form_exited_staging, mobile_form_outcome_received_staging, mobile_form_started_staging,
 mobile_form_submitted_staging, mobile_product_clicked_staging, mobile_product_viewed_staging,
 mobile_screen_engaged_staging, mobile_screen_refreshed_staging, mobile_screen_viewed_staging,
 form_outcome_received_staging, traffic_sources_staging, form_submitted_staging,
 mobile_application_backgrounded, mobile_lpupdated_staging, mobile_MXresponse_captured_staging] >> tpg_app_staging_tables

# Amex Business Dependencies
[ot_raw_staging, ot_metadata_raw_staging] >> amex_ot_details_staging
amex_ot_details_staging >> amex_ot_summary_staging

[page_view_staging, page_metrics_staging, product_clicked_staging, product_viewed_staging, element_clicked_staging,
 element_viewed_staging, device_staging, location_staging, decsion_staging, traffic_sources_staging,
 form_submitted_staging, paidsearch_staging, cookies_staging, pzn_offers_received_staging, twilio_call_connected_staging,
 twilio_call_transferred_staging, amex_ot_summary_staging] >> amex_business_staging_tables

# Amex Consumer Dependencies
[page_view_staging, page_metrics_staging, product_clicked_staging, product_viewed_staging, element_clicked_staging,
 element_viewed_staging, device_staging, location_staging, decsion_staging, traffic_sources_staging, paidsearch_staging,
 cookies_staging, pqo_offer_received_staging, pzn_offers_received_staging, pqo_offer_requested_staging,
 amex_ot_summary_staging] >> amex_consumer_staging_tables

# Lonely Planet Dependencies
[session_staging, page_view_staging, page_metrics_staging] >> lp_staging_tables

# COF Reporting Dependencies
[session_staging, page_view_staging, page_metrics_staging] >> cof_staging_tables
