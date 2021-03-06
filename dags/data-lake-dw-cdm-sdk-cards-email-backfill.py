from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from operators.finserv_cdm_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10),
    'email': ['rshukla@redventures.com'],
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
DAG_NAME = 'data-lake-dw-cdm-sdk-cards-email-backfill'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s/%s' % (ACCOUNT, Variable.get("log-environment"), DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
small_task_custom_cluster = {
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
        'ebs_volume_count': 1,
        'ebs_volume_size': 100,
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

medium_task_custom_cluster = {
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
        'ebs_volume_size': 200,
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
        'ebs_volume_count': 2,
        'ebs_volume_size': 200,
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
reporting_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/" + Variable.get("environment") + "/scala-2.12/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
    }
]

email_campaign_dim_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailCampaignDim",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_link_dim_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailLinkDim",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_signup_source_dim_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailSignupSourceDim",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_subject_dim_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailSubjectDim",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_template_dim_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailTemplateDim",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_unsub_source_dim_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailUnsubSourceDim",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_workflow_dim_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailWorkflowDim",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_sub_dim_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailSubDim",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_event_fct_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailEventFct",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

email_click_fct_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_FROMDATE"),
        "END_DATE=" + Variable.get("CDM_EMAIL_BACKFILL_LOOKBACK_TODATE"),
        "TABLES=" + "com.redventures.cdm.email.warehouse.EmailClickFct",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_EMAIL_TENANTS"),
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
    ]
}

# DAG Creation Step
with DAG(DAG_NAME,
         schedule_interval=None,
         dagrun_timeout=timedelta(hours=6),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    email_campaign_dim_task = FinServDatabricksSubmitRunOperator(
        task_id='email-campaign-dim',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_campaign_dim_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_link_dim_task = FinServDatabricksSubmitRunOperator(
        task_id='email-link-dim',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_link_dim_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_signup_source_dim_task = FinServDatabricksSubmitRunOperator(
        task_id='email-signup-source-dim',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_signup_source_dim_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_subject_dim_task = FinServDatabricksSubmitRunOperator(
        task_id='email-subject-dim',
        new_cluster=small_task_custom_cluster,
        spark_jar_task=email_subject_dim_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_template_dim_task = FinServDatabricksSubmitRunOperator(
        task_id='email-template-dim',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_template_dim_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_unsub_source_dim_task = FinServDatabricksSubmitRunOperator(
        task_id='email-unsub-source-dim',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_unsub_source_dim_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_workflow_dim_task = FinServDatabricksSubmitRunOperator(
        task_id='email-workflow-dim',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_workflow_dim_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_sub_dim_task = FinServDatabricksSubmitRunOperator(
        task_id='email-sub-dim',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_sub_dim_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_event_fct_task = FinServDatabricksSubmitRunOperator(
        task_id='email-event-fct',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_event_fct_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    email_click_fct_task = FinServDatabricksSubmitRunOperator(
        task_id='email-click-fct',
        new_cluster=medium_task_custom_cluster,
        spark_jar_task=email_click_fct_jar_task,
        libraries=reporting_libraries,
        timeout_seconds=10800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
