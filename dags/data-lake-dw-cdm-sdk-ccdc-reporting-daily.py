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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'op_kwargs': cfg_dict,
    'provide_context': True
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-lake-dw-cdm-sdk-ccdc-reporting-daily'

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
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

old_medium_task_cluster = {
    'spark_version': '5.3.x-scala2.11',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': Variable.get("DBX_MEDIUM_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'cluster_log_conf': LOG_PATH,
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com',
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
        "instance_profile_arn": Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com',
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
        "instance_profile_arn": Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com',
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
        "jar": "dbfs:/data-warehouse/production/datawarehouse-builder-0.8.1-tmp.jar",
    },
]

reporting_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/" + Variable.get("environment") + "scala-2.12/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
    }
]

# Reporting table tasks
conversion_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("CCDC_HOURLY_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.ccdc.reporting.Conversion",
        "ACCOUNT=" + Variable.get("DBX_CCDC_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CCDC_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

session_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("CCDC_HOURLY_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.ccdc.reporting.Session",
        "ACCOUNT=" + Variable.get("DBX_CCDC_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CCDC_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

product_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("CCDC_DAILY_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.ccdc.reporting.Product",
        "ACCOUNT=" + Variable.get("DBX_CCDC_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CCDC_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

page_view_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("CCDC_HOURLY_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.ccdc.reporting.PageView",
        "ACCOUNT=" + Variable.get("DBX_CCDC_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CCDC_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

attribution_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=730))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.ccdc.reporting.Attribution",
        "ACCOUNT=" + Variable.get("DBX_CCDC_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CCDC_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

anonymous_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=730))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.ccdc.reporting.Anonymous",
        "ACCOUNT=" + Variable.get("DBX_CCDC_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_CCDC_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

session_outcomes_update_reporting_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "lookBackDays": Variable.get("CCDC_DAILY_LOOKBACK_DAYS"),
        "environment": "staging",
        "stagingPath": Variable.get("DBX_CARDS_Staging_Path"),
        "reportingPath": Variable.get("DBX_CCDC_Reporting_Path"),
        "dimensionPath": Variable.get("DBX_Dimensions_Path"),
        "loggingPath": Variable.get("DBX_CCDC_Logging_Path"),
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "tenantName": "ccdc",
        "tableName": "Session",
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/' + Variable.get("DBX_CCDC_CODE_ENV") + '/reporting-table-notebooks/OutcomesUpdate',
}


page_view_outcomes_update_reporting_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "lookBackDays": Variable.get("CCDC_DAILY_LOOKBACK_DAYS"),
        "environment": "staging",
        "stagingPath": Variable.get("DBX_CARDS_Staging_Path"),
        "reportingPath": Variable.get("DBX_CCDC_Reporting_Path"),
        "dimensionPath": Variable.get("DBX_Dimensions_Path"),
        "loggingPath": Variable.get("DBX_CCDC_Logging_Path"),
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "tenantName": "ccdc",
        "tableName": "PageView",
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/' + Variable.get("DBX_CCDC_CODE_ENV") + '/reporting-table-notebooks/OutcomesUpdate',
}

conversion_outcomes_update_reporting_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "lookBackDays": Variable.get("CCDC_DAILY_LOOKBACK_DAYS"),
        "environment": "staging",
        "stagingPath": Variable.get("DBX_CARDS_Staging_Path"),
        "reportingPath": Variable.get("DBX_CCDC_Reporting_Path"),
        "dimensionPath": Variable.get("DBX_Dimensions_Path"),
        "loggingPath": Variable.get("DBX_CCDC_Logging_Path"),
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "tenantName": "ccdc",
        "tableName": "Conversion",
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/' + Variable.get("DBX_CCDC_CODE_ENV") + '/reporting-table-notebooks/OutcomesUpdate',
}

# DAG Creation Step
with DAG('data-lake-dw-cdm-sdk-ccdc-reporting-daily',
         schedule_interval='0 8 * * *',
         dagrun_timeout=timedelta(hours=3),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    ccdc_staging_tables = ExternalTaskSensor(
        task_id='external-ccdc-reporting',
        external_dag_id='data-lake-dw-cdm-sdk-cards-staging-daily',
        external_task_id='external-ccdc-staging',
        execution_timeout=timedelta(minutes=10),
        execution_delta=timedelta(minutes=30)
    )

    conversion_reporting = FinServDatabricksSubmitRunOperator(
        task_id='conversion-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=conversion_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    session_reporting = FinServDatabricksSubmitRunOperator(
        task_id='session-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=session_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_reporting = FinServDatabricksSubmitRunOperator(
        task_id='product-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=product_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    page_view_reporting = FinServDatabricksSubmitRunOperator(
        task_id='page-view-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=page_view_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    anonymous_reporting = FinServDatabricksSubmitRunOperator(
        task_id='anonymous-reporting',
        new_cluster=medium_task_cluster,
        spark_jar_task=anonymous_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=8400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    attribution_reporting = FinServDatabricksSubmitRunOperator(
        task_id='attribution-reporting',
        new_cluster=large_task_cluster,
        spark_jar_task=attribution_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=8400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    session_outcomes_update_reporting = FinServDatabricksSubmitRunOperator(
        task_id='session-outcomes-update-reporting',
        new_cluster=old_medium_task_cluster,
        notebook_task=session_outcomes_update_reporting_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    conversion_outcomes_update_reporting = FinServDatabricksSubmitRunOperator(
        task_id='conversion-outcomes-update-reporting',
        new_cluster=old_medium_task_cluster,
        notebook_task=conversion_outcomes_update_reporting_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    page_view_outcomes_update_reporting = FinServDatabricksSubmitRunOperator(
        task_id='page-view-outcomes-update-reporting',
        new_cluster=old_medium_task_cluster,
        notebook_task=page_view_outcomes_update_reporting_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )


# Dependencies
ccdc_staging_tables >> [conversion_reporting, session_reporting, page_view_reporting, product_reporting]

# outcomes update reporting dependencies
conversion_reporting >> conversion_outcomes_update_reporting
session_reporting >> session_outcomes_update_reporting
page_view_reporting >> page_view_outcomes_update_reporting

# Anonymous reporting dependencies
session_outcomes_update_reporting >> anonymous_reporting

# Attribution reporting dependencies
session_outcomes_update_reporting >> attribution_reporting
