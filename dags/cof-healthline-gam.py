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
    'aws_attributes': {
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        'availability': 'SPOT_WITH_FALLBACK',
        'instance_profile_arn': Variable.get("DBX_CARDS_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': ' B532',
        'Project': 'CreditCards.com',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

large_task_custom_cluster = {
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
gam_mapping_large_task_custom_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'm5a.2xlarge',
    'driver_node_type_id': 'm5a.2xlarge',
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
staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/" + Variable.get("environment") + "/scala-2.12/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
    },
]

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

hl_staging_gam_lineitem_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.gam.healthline.reporting.GamLineItem",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("HL_GAM_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

hl_staging_gam_adunit_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.gam.healthline.reporting.GamAdUnit",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("HL_GAM_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

hl_staging_gam_order_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.gam.healthline.reporting.GamOrder",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("HL_GAM_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

hl_staging_gam_impressions_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.gam.healthline.reporting.GamImpressions",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("HL_GAM_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__HLORDERIDS=" + Variable.get("DBX_GAM_HLORDERIDS")
    ]
}

hl_staging_gam_clicks_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.gam.healthline.reporting.GamClicks",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("HL_GAM_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__HLORDERIDS=" + Variable.get("DBX_GAM_HLORDERIDS")
    ]
}

hl_staging_funnel_ids_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.staging.HLFunnelIds",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__HLORDERIDS=" + Variable.get("DBX_GAM_HLORDERIDS")
    ]
}

hl_reporting_funnel_metrics_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.HLFunnelMetrics",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__HLORDERIDS=" + Variable.get("DBX_GAM_HLORDERIDS")
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

    hl_gam_mapping_report_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-healthline-gam-mapping',
        new_cluster=gam_mapping_large_task_custom_cluster,
        spark_jar_task=cof_report_hl_mapping_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    hl_gam_staging_lineitem_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-gam-staging-lineitem',
        new_cluster=small_task_cluster,
        spark_jar_task=hl_staging_gam_lineitem_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    hl_gam_staging_adunit_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-gam-staging-adunit',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=hl_staging_gam_adunit_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    hl_gam_staging_order_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-gam-staging-order',
        new_cluster=small_task_cluster,
        spark_jar_task=hl_staging_gam_order_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    hl_gam_impressions_task = FinServDatabricksSubmitRunOperator(
        task_id='hl-staging-gam-impressions',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=hl_staging_gam_impressions_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    hl_gam_clicks_task = FinServDatabricksSubmitRunOperator(
        task_id='hl-staging-gam-clicks',
        new_cluster=small_task_cluster,
        spark_jar_task=hl_staging_gam_clicks_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    hl_gam_staging_funnel_ids_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-gam-staging-funnel-ids',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=hl_staging_funnel_ids_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    hl_gam_reporting_funnel_metrics_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-gam-reporting-funnel-metrics',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=hl_reporting_funnel_metrics_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

# dependencies
[hl_gam_impressions_task, hl_gam_clicks_task] >> hl_gam_mapping_report_task
hl_gam_staging_funnel_ids_task >> hl_gam_reporting_funnel_metrics_task
[hl_gam_staging_lineitem_task, hl_gam_mapping_report_task] >> hl_gam_staging_funnel_ids_task
