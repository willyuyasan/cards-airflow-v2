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
DAG_NAME = 'data-lake-dw-cdm-sdk-cof-reporting-hourly'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s/%s' % (ACCOUNT, Variable.get("environment"), DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
small_task_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'num_workers': Variable.get("DBX_SMALL_CLUSTER_NUM_NODES"),
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
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B814',
        'Project': 'Cards Allocation',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

medium_task_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': Variable.get("DBX_MEDIUM_CLUSTER_NUM_NODES"),
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
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B814',
        'Project': 'Cards Allocation',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

large_task_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
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
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B814',
        'Project': 'Cards Allocation',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

# Libraries
reporting_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/" + Variable.get("environment") + "/scala-2.12/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
    }
]

session_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.COFSession",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

page_view_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.COFPageView",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

anonymous_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.COFAnonymous",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

productclicked_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.ProductClicked",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

productviewed_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.ProductViewed",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

ProductListViewed_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.ProductListViewed",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

elementclicked_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.ElementClicked",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

elementviewed_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.ElementViewed",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

partner_cookie_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.PartnerCookieCaptured",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

segment_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_COF_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_COF_COHESION_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.SegmentAudience",
        "ACCOUNT=" + Variable.get("DBX_COF_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

# DAG Creation Step
with DAG('data-lake-dw-cdm-sdk-cof-reporting-hourly',
         schedule_interval='0 0-6,11-23 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    cof_staging_tables = ExternalTaskSensor(
        task_id='external-cof-reporting',
        external_dag_id='data-lake-dw-cdm-sdk-cards-staging-hourly',
        external_task_id='external-cof-staging',
        execution_timeout=timedelta(minutes=10),
        execution_delta=timedelta(minutes=30)
    )

    session_reporting = FinServDatabricksSubmitRunOperator(
        task_id='session-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=session_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    page_view_reporting = FinServDatabricksSubmitRunOperator(
        task_id='page-view-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=page_view_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    anonymous_reporting = FinServDatabricksSubmitRunOperator(
        task_id='anonymous-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=anonymous_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_viewed_reporting = FinServDatabricksSubmitRunOperator(
        task_id='product-viewed-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=productviewed_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_clicked_reporting = FinServDatabricksSubmitRunOperator(
        task_id='product-clicked-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=productclicked_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_ListViewed_reporting = FinServDatabricksSubmitRunOperator(
        task_id='product-ListViewed-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=ProductListViewed_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    element_viewed_reporting = FinServDatabricksSubmitRunOperator(
        task_id='element-viewed-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=elementviewed_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    element_clicked_reporting = FinServDatabricksSubmitRunOperator(
        task_id='element-clicked-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=elementclicked_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    partner_cookie_reporting = FinServDatabricksSubmitRunOperator(
        task_id='partner-cookie-reporting',
        new_cluster=large_task_cluster,
        spark_jar_task=partner_cookie_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    segment_audience_reporting = FinServDatabricksSubmitRunOperator(
        task_id='segment-audience-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=segment_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# Dependencies
cof_staging_tables >> [session_reporting, page_view_reporting]
[session_reporting, page_view_reporting] >> anonymous_reporting
