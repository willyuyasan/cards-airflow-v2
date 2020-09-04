from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators import ExternalTaskSensor
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email': ['vmalhotra@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'op_kwargs': cfg_dict,
    'provide_context': True
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"

# Cluster Setup Step
small_i3_x_1w_task_custom_cluster = {
    'spark_version':            '5.3.x-scala2.11',
    'node_type_id':             'm5.large',
    'driver_node_type_id':      'm5.large',
    'num_workers':              1,
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf':    'dbfs://home/cluster_log',
    'spark_conf': {
      'spark.sql.sources.partitionOverwriteMode': 'dynamic',
      'spark.driver.extraJavaOptions': '-Dconfig.resource=application-cards-qa.conf',
      'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
      'java_opts': '-Dconfig.resource=application-cards-qa.conf'
    },
    "aws_attributes": {
        "availability":             "SPOT_WITH_FALLBACK",
        'ebs_volume_count':         2,
        'ebs_volume_size':          100,
        'ebs_volume_type':          'GENERAL_PURPOSE_SSD',
        'first_on_demand':          '2',
        'spot_bid_price_percent':   '60',
        'zone_id':                  'us-east-1c',
        "instance_profile_arn":     Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com'
    },
}

small_i3_x_1w_task_cohesion_cluster = {
    'spark_version':            '5.3.x-scala2.11',
    'node_type_id':             'm5.large',
    'driver_node_type_id':      'm5.large',
    'num_workers':              1,
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf':    'dbfs://home/cluster_log',
    'spark_conf': {
      'spark.sql.sources.partitionOverwriteMode': 'dynamic',
      'spark.driver.extraJavaOptions': '-Dconfig.resource=application-cohesion-dev.conf',
      'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
      'java_opts': '-Dconfig.resource=application-cohesion-dev.conf'
    },
    "aws_attributes": {
        "availability":             "SPOT_WITH_FALLBACK",
        'ebs_volume_count':         2,
        'ebs_volume_size':          100,
        'ebs_volume_type':          'GENERAL_PURPOSE_SSD',
        'first_on_demand':          '2',
        'spot_bid_price_percent':   '60',
        'zone_id':                  'us-east-1c',
        "instance_profile_arn":     Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com'
    },
}


medium_i3_x_3w_task_cluster = {
    'spark_version':            '5.3.x-scala2.11',
    'node_type_id':             'm5.large',
    'driver_node_type_id':      'm5.large',
    'num_workers':              3,
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf':    'dbfs://home/cluster_log',
    'spark_conf': {
      'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    "aws_attributes": {
        "availability":             "SPOT_WITH_FALLBACK",
        'ebs_volume_count':         3,
        'ebs_volume_size':          100,
        'ebs_volume_type':          'GENERAL_PURPOSE_SSD',
        'first_on_demand':          '2',
        'spot_bid_price_percent':   '70',
        'zone_id':                  'us-east-1c',
        "instance_profile_arn":     Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com'
    },
}

large_i3_2x_6w_task_cluster = {
    'spark_version':            '5.3.x-scala2.11',
    'node_type_id':             'i3.2xlarge',
    'driver_node_type_id':      'i3.2xlarge',
    'num_workers':              6,
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf':    'dbfs://home/cluster_log',
    'spark_conf': {
      'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    "aws_attributes": {
        "availability":             "SPOT_WITH_FALLBACK",
        'ebs_volume_count':         6,
        'ebs_volume_size':          100,
        'ebs_volume_type':          'GENERAL_PURPOSE_SSD',
        'first_on_demand':          '2',
        'spot_bid_price_percent':   '70',
        'zone_id':                  'us-east-1c',
        "instance_profile_arn":     Variable.get("DBX_CCDC_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': 'B530',
        'Project': 'CreditCards.com'
    },
}

# Libraries
staging_libraries = [
  {
    "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
  },
  {
    "jar": "dbfs:/Libraries/JVM/cdm-data-mart-cards/cdm-data-mart-cards-assembly-0.0.1-SNAPSHOT.jar",
  },
]

page_metrics_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime("%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.PageMetrics",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CCDC_Tenant_Id"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

# DAG Creation Step
with DAG(
    'data-lake-dw-cdm-sdk-ccdc-reporting-hourly',
    schedule_interval='0 0-5,9-23 * * *',
    dagrun_timeout=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args) as dag:

    ccdc_staging_tables = ExternalTaskSensor(
        task_id='external-ccdc-reporting',
        external_dag_id='data-lake-dw-cdm-sdk-cards-staging-hourly-workflow',
        external_task_id='external-ccdc-staging',
        execution_timeout=timedelta(minutes=7),
        execution_delta=timedelta(minutes=30)
    )

    page_metrics_staging = DatabricksSubmitRunOperator(
        task_id='page-metrics-staging',
        new_cluster=small_i3_x_1w_task_custom_cluster,
        spark_jar_task=page_metrics_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# Dependencies
ccdc_staging_tables >> page_metrics_staging
