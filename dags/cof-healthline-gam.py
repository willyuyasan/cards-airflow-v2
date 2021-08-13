from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 08, 01),
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

#tenants
tenants =   ("4b075c45-81db-4c14-8c72-46a1eff91981",
               "89144c93-5662-4fb8-b8eb-0a2140dc52f0",
               "src_1jbobeEHGTZmBD9QsSnBOaHCzpy",
               "src_1kYriYWtbLbCMPKD70alLfKJrWO",
               "src_1kZIrJza0Ger6IH4Oxfqi9McDOx",
               "f68ce971-1a80-4e60-bcc7-998224286138",
               "a80f134c-8aa7-482f-bce1-7c8af6e0a271",
               "3a96cef3-4355-4236-b5ba-7efdc2a1e552",
               "64bf0ba4-8b94-4188-8645-6703f8b33f0a",
               "ab687b54-79c7-44d5-b6dc-455689b7e2b5",
               "ad5746ee-d875-40bd-906d-77b046bb1f45",
               "7350d3bf-18d7-4d19-9917-66b8423efebf",
               "e5b42ea4-4f7c-499e-afa4-837028aca12f",
               "2c9f1797-a23b-4051-9fff-68d10990d658",
               "5c94f1db-0662-4d7f-b894-83e7d3bf028f",
               "5cd2036c-8780-4d65-896a-fb2612c2a17d",
               "0efcde65-4fb8-4aa5-9b17-4c641d5f9e3b",
               "8830ce36-13f9-40da-8176-b8cbf73a598c",
               "src_1lg1SLNP84Uk7Ocf2fMZGpo1k10",
               "src_1NT29eAqcol4XeySBENzOtJn2jj",
               "a04f810b-eb6f-46a9-8842-e1f9b98ae026",
               "src_1WWpnfn4eOBQ0a9YmXEux3ZQCLx",
               "dc0c22a1-ea8a-45ae-ae9a-f63379366a5")




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

# Libraries
staging_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/FileStore/jars/d69470f3_6b85_4b6a_9f84_677fed6a9631-cdm_cof_assembly_1_0_6-e0007.jar",
    },
]

credit_report_jar_task = {
    'main_class_name': "com.redventures.cdm.cof.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
                datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CREDIT_REPORT_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cof.staging.GamByOrderIds",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + tenants,
        "WRITE_BUCKET=" + Variable.get("DBX_CARDS_Bucket")
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

    credit_report_task = FinServDatabricksSubmitRunOperator(
        task_id='cof-healthline-gam',
        new_cluster=large_task_custom_cluster,
        spark_jar_task=credit_report_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
