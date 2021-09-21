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
DAG_NAME = 'data-lake-dw-cdm-sdk-cof-cnet'

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
        'spark.driver.extraJavaOptions': '-Dconfig.resource=' + Variable.get("SDK_CONFIG_FILE"),
        'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
        'java_opts': '-Dconfig.resource=' + Variable.get("SDK_CONFIG_FILE"),
        'GOOGLE_APPLICATION_CREDENTIALS': '/dbfs/gcp/rv-mt-data-prod-svc-key.json'
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
small_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': Variable.get("DBX_MEDIUM_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf': 'dbfs://home/cluster_log',
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'spark_env_vars': {
        'GOOGLE_APPLICATION_CREDENTIALS': '/dbfs/gcp/rv-mt-data-prod-svc-key.json'
    },
    'aws_attributes': {
        'availability': 'SPOT_WITH_FALLBACK',
        'instance_profile_arn': Variable.get("DBX_CARDS_IAM_ROLE"),
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c'
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

notebook_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-timeinc/cdm-timeinc-assembly-0.0.14.jar",
    },
]

cnet_gam_notebook_task = {
    'base_parameters': {
        "Environment": Variable.get("DBX_CNET_CODE_ENV"),
        "endDate": datetime.now().strftime("%Y-%m-%d"),
        "redshiftWriteMode": "insert-only",
        "extraLookbackDays": "1",
        "s3WriteMode": "overwrite",
        "startDate": (
                datetime.now() - (timedelta(days=int(int(Variable.get("CNET_GAM_Lookback")))))).strftime(
            "%Y-%m-%d"),
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/production-environment/staging-table-notebooks/cnet_gam_data_pull',
}

cof_report_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.CnetGamMapping",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__crosssitelookback=" + Variable.get("DBX_gam_crosssite_lookback_days")
    ]
}
cnet_staging_funnel_ids_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.staging.CnetFunnelIds",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CNET_COF_Tenant_Ids"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__CNET_GAM_ORDER_IDS=" + Variable.get("DBX_GAM_CNET_ORDERIDS"),
        "CUSTOM_PARAMETERS__CNET_OP_ORDER_IDS=" + Variable.get("DBX_OP_CNET_ORDERIDS")
    ]
}

cnet_reporting_funnel_metrics_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_SDK_GAM_Lookback_Days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.cof.reporting.CnetFunnelMetrics",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CNET_COF_Tenant_Ids"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
        "CUSTOM_PARAMETERS__CNET_GAM_ORDER_IDS=" + Variable.get("DBX_GAM_CNET_ORDERIDS"),
        "CUSTOM_PARAMETERS__CNET_OP_ORDER_IDS=" + Variable.get("DBX_OP_CNET_ORDERIDS")
    ]
}

cof_aam_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "daily",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("CNET_GAM_Lookback")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.aam.reporting.SegmentsDim" +
                    ",com.redventures.cdm.datamart.cards.aam.reporting.TraitsDim" +
                    ",com.redventures.cdm.datamart.cards.aam.staging.Visitors",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket"),
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

    cnet_gam_data_task = FinServDatabricksSubmitRunOperator(
        task_id='cnet-gam-data-pull',
        new_cluster=small_task_cluster,
        notebook_task=cnet_gam_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    cnet_gam_mapping_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-cnet-gam-mapping',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=cof_report_jar_task,
        libraries=staging_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    cnet_aam_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-cnet-aam-task',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=cof_aam_jar_task,
        libraries=staging_libraries,
        timeout_seconds=7200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    cnet_gam_staging_funnel_ids_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-gam-staging-funnel-ids',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=cnet_staging_funnel_ids_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

    cnet_gam_reporting_funnel_metrics_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-gam-reporting-funnel-metrics',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=cnet_reporting_funnel_metrics_task,
        libraries=staging_libraries,
        timeout_seconds=9600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=60
    )

# Defining  dependencies
cnet_gam_data_task >> cnet_gam_mapping_task
cnet_gam_staging_funnel_ids_task >> cnet_gam_reporting_funnel_metrics_task
