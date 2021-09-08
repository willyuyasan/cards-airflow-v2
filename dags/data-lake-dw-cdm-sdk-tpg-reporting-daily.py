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
    'provide_context': True,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-lake-dw-cdm-sdk-tpg-reporting-daily'

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

medium_task_cluster = {
    'spark_version': '5.3.x-scala2.11',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': 4,
    'auto_termination_minutes': 0,
    'cluster_log_conf': LOG_PATH,
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        "ebs_volume_count": 2,
        "ebs_volume_size": 100,
        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
        "first_on_demand": "2",
        "spot_bid_price_percent": "70",
        "zone_id": "us-east-1c",
        "instance_profile_arn": Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B532',
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

new_medium_task_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
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
        "ebs_volume_count": 2,
        "ebs_volume_size": 100,
        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
        "first_on_demand": "2",
        "spot_bid_price_percent": "70",
        "zone_id": "us-east-1c",
        "instance_profile_arn": Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B532',
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

large_5w_task_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'num_workers': 5,
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
        "ebs_volume_count": 2,
        "ebs_volume_size": 100,
        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
        "first_on_demand": "2",
        "spot_bid_price_percent": "70",
        "zone_id": "us-east-1c",
        "instance_profile_arn": Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B532',
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

large_11w_task_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'i3.4xlarge',
    'driver_node_type_id': 'i3.4xlarge',
    'num_workers': 11,
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
    'aws_attributes': {
        'ebs_volume_count': 9,
        'ebs_volume_size': 400,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        'availability': 'SPOT_WITH_FALLBACK',
        'instance_profile_arn': Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B532',
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

base_params_staging = {
    "lookBackDays": Variable.get("TPG_SHORT_LOOKBACK_DAYS"),
    "environment": "staging",
    "stagingPath": Variable.get("DBX_CARDS_Staging_Path"),
    "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
    "dimensionPath": Variable.get("DBX_TPG_Dimension_Path"),
    "loggingPath": Variable.get("DBX_TPG_Logging_Path"),
    "dataLakePath": Variable.get("DBX_DataLake_Path"),
    "tenantName": "tpg",
    "toDate": "now"
}

base_params_reporting = {
    "lookBackDays": Variable.get("TPG_LONG_LOOKBACK_DAYS"),
    "environment": "reporting",
    "stagingPath": Variable.get("DBX_CARDS_Staging_Path"),
    "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
    "dimensionPath": Variable.get("DBX_TPG_Dimension_Path"),
    "loggingPath": Variable.get("DBX_TPG_Logging_Path"),
    "dataLakePath": Variable.get("DBX_DataLake_Path"),
    "tenantName": "tpg",
    "toDate": "now",
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
session_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("TPG_LONG_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.Session",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

conversion_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("TPG_LONG_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.Conversion",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

product_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("TPG_LONG_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.Product",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__redshift_Partitioned_Days=" + Variable.get("DBX_TPG_Product_Redshift_Partitoned_Upload_Days")
    ]
}

page_view_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("TPG_LONG_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.PageView",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__redshift_Partitioned_Days=" + Variable.get("DBX_TPG_PageView_Redshift_Partitoned_Upload_Days")
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
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.Attribution",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__redshift_Partitioned_Days=" + Variable.get("DBX_TPG_Attribution_Redshift_Partitoned_Upload_Days")

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
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.Anonymous",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__redshift_Partitioned_Days=" + Variable.get("DBX_TPG_Anonymous_Redshift_Partitoned_Upload_Days")
    ]
}

amp_reporting_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("TPG_LONG_LOOKBACK_DAYS")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.tpg.reporting.AmpPageView",
        "ACCOUNT=" + Variable.get("DBX_TPG_Account"),
        "WRITE_BUCKET=" + Variable.get("DBX_TPG_Bucket"),
        "READ_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

# dimension base params
dimension_tables_notebook_task['base_parameters'].update(base_params_staging)

# DAG Creation Step
with DAG('data-lake-dw-cdm-sdk-tpg-reporting-daily',
         schedule_interval='0 9 * * *',
         dagrun_timeout=timedelta(hours=4),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    tpg_staging_tables = ExternalTaskSensor(
        task_id='external-tpg-reporting',
        external_dag_id='data-lake-dw-cdm-sdk-cards-staging-daily',
        external_task_id='external-tpg-staging',
        execution_timeout=timedelta(minutes=15),
        execution_delta=timedelta(minutes=30)
    )

    dimension_tables = FinServDatabricksSubmitRunOperator(
        task_id='dimension-tables',
        new_cluster=medium_task_cluster,
        notebook_task=dimension_tables_notebook_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    conversion_reporting = FinServDatabricksSubmitRunOperator(
        task_id='conversion-reporting',
        new_cluster=large_5w_task_cluster,
        spark_jar_task=conversion_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    session_reporting = FinServDatabricksSubmitRunOperator(
        task_id='session-reporting',
        new_cluster=large_5w_task_cluster,
        spark_jar_task=session_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    page_view_reporting = FinServDatabricksSubmitRunOperator(
        task_id='page-view-reporting',
        new_cluster=large_5w_task_cluster,
        spark_jar_task=page_view_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=9000,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_reporting = FinServDatabricksSubmitRunOperator(
        task_id='product-reporting',
        new_cluster=large_5w_task_cluster,
        spark_jar_task=product_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=9000,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    amp_reporting = FinServDatabricksSubmitRunOperator(
        task_id='amp-reporting',
        new_cluster=new_medium_task_cluster,
        spark_jar_task=amp_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=8400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    anonymous_reporting = FinServDatabricksSubmitRunOperator(
        task_id='anonymous-reporting',
        new_cluster=large_5w_task_cluster,
        spark_jar_task=anonymous_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=14000,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    attribution_reporting = FinServDatabricksSubmitRunOperator(
        task_id='attribution-reporting',
        new_cluster=large_11w_task_cluster,
        spark_jar_task=attribution_reporting_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=22000,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )


# Dependencies
tpg_staging_tables >> [dimension_tables, product_reporting, page_view_reporting, session_reporting]

dimension_tables >> conversion_reporting

conversion_reporting >> amp_reporting
session_reporting >> [anonymous_reporting, attribution_reporting]
