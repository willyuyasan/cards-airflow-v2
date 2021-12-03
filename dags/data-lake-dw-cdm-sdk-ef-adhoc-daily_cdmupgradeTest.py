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
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-travel-name")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'op_kwargs': cfg_dict,
    'provide_context': True,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-lake-dw-cdm-sdk-ef-adhoc-daily_cdmupgradeTest'

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

reporting_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/FileStore/jars/ac16e185_1677_4f8d_927d_0933a72a14f2-cdm_data_mart_cards_assembly_0_0_1_SNAPSHOT-97c7e.jar",
    }
]

# Reporting table tasks
premium_availability_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(2))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocPremiumAvailability",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

two_day_window_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocTwoDayWindow",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

two_day_window_only_success_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocTwoDayWindowOnlySuccess",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

seven_day_window_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocSevenDayWindow",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

seven_day_window_only_success_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocSevenDayWindowOnlySuccess",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

usage_report_non_alert_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(100))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocUsageReportNonAlert",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

alert_report_alert_created_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(100))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocAlertReportAlertCreated",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

alert_report_alert_evaluated_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(100))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocAlertReportAlertEvaluated",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

alert_report_alert_evaluated_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(100))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocAlertReportAlertEvaluated",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

alert_report_execution_summary_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(10))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocAlertReportExecutionSummary",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

alert_report_faa_active_alerts_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(100))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocAlertReportFAActiveAlerts",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

alert_report_saa_active_alerts_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(100))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocAlertReportSAActiveAlerts",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

alert_report_aca_active_alerts_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(100))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_EF_Tenant_Id"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.expert_flyer.reporting.AdHocAlertReportACAActiveAlerts",
        "ACCOUNT=" + "cards",
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "READ_BUCKET=" + "rv-core-pipeline"
    ]
}

# DAG Creation Step
with DAG(DAG_NAME,
         schedule_interval='0 10 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    premium_availability = FinServDatabricksSubmitRunOperator(
        task_id='premium-availability',
        new_cluster=small_task_cluster,
        spark_jar_task=premium_availability_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    two_day_window = FinServDatabricksSubmitRunOperator(
        task_id='two-day-window',
        new_cluster=small_task_cluster,
        spark_jar_task=two_day_window_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    two_day_window_only_success = FinServDatabricksSubmitRunOperator(
        task_id='two-day-window-only_success',
        new_cluster=small_task_cluster,
        spark_jar_task=two_day_window_only_success_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    seven_day_window = FinServDatabricksSubmitRunOperator(
        task_id='seven-day-window',
        new_cluster=small_task_cluster,
        spark_jar_task=seven_day_window_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    seven_day_window_only_success = FinServDatabricksSubmitRunOperator(
        task_id='seven-day-window-only-success',
        new_cluster=small_task_cluster,
        spark_jar_task=seven_day_window_only_success_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    usage_report_non_alert = FinServDatabricksSubmitRunOperator(
        task_id='usage-report-non-alert',
        new_cluster=small_task_cluster,
        spark_jar_task=usage_report_non_alert_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    alert_report_alert_created = FinServDatabricksSubmitRunOperator(
        task_id='alert-report-alert-created',
        new_cluster=small_task_cluster,
        spark_jar_task=alert_report_alert_created_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    alert_report_alert_evaluated = FinServDatabricksSubmitRunOperator(
        task_id='alert-report-alert-evaluated',
        new_cluster=small_task_cluster,
        spark_jar_task=alert_report_alert_evaluated_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    alert_report_execution_summary = FinServDatabricksSubmitRunOperator(
        task_id='alert-report-execution-summary',
        new_cluster=small_task_cluster,
        spark_jar_task=alert_report_execution_summary_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    alert_report_faa_active_alerts = FinServDatabricksSubmitRunOperator(
        task_id='alert_report-faa-active-alerts',
        new_cluster=small_task_cluster,
        spark_jar_task=alert_report_faa_active_alerts_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    alert_report_saa_active_alerts = FinServDatabricksSubmitRunOperator(
        task_id='alert_report-saa-active-alerts',
        new_cluster=small_task_cluster,
        spark_jar_task=alert_report_saa_active_alerts_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    alert_report_aca_active_alerts = FinServDatabricksSubmitRunOperator(
        task_id='alert_report-aca-active-alerts',
        new_cluster=small_task_cluster,
        spark_jar_task=alert_report_aca_active_alerts_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )


# Dependencies
