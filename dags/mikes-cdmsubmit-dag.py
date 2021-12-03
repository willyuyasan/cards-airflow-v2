from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from rvairflow.dbx.dbx_operator import CdmDatabricksSubmitRunOperator
from rvairflow.dbx.task import NewCluster, JarTask, NotebookParams, NotebookTask, SparkEnvVars, ClusterCustomTags, DagDefaultArgs
from rvairflow.cdm.params import RunnerParams
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email': ['vmalhotra@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': sh.slack_failure_callback(slack_connection_id='{{ var.value.slack-connection-name }}'),
    # 'retries': 2,
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
            ACCOUNT, '{{ var.value.environment }}', DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
small_task_custom_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'm5a.xlarge',
    'driver_node_type_id': 'm5a.xlarge',
    'num_workers': 4,
    # 'auto_termination_minutes': 0,
    # 'cluster_log_conf': LOG_PATH,
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.driver.extraJavaOptions': '-Dconfig.resource=' + Variable.get("SDK_CONFIG_FILE"),
        'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
        'java_opts': '-Dconfig.resource=' + Variable.get("SDK_CONFIG_FILE")
    },
    # "aws_attributes": {
    "availability": "SPOT_WITH_FALLBACK",
    'ebs_volume_count': 1,
    'ebs_volume_size': 100,
    'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
    'first_on_demand': '2',
    'spot_bid_price_percent': '60',
    'zone_id': 'us-east-1b',
    "instance_profile_arn": Variable.get("DBX_CARDS_IAM_ROLE")  # "'{{ var.value.DBX_CARDS_IAM_ROLE }}',
    # },
    # 'custom_tags': {
    #     'Partner': 'B814',
    #     'Project': 'Cards Allocation',
    #     'Dag_id': "{{ ti.dag_id }}",
    #     'Task_id': "{{ ti.task_id }}"
    # },
}
ct = ClusterCustomTags(cluster_type="Development", partner="B814", product="DatabricksDevelopment", owner="cdm", created_by='cdm-databricks_svc')
env = SparkEnvVars(cdm_secret_scope='cards', api_secret_scope='cards')
cluster = NewCluster(spark_env_obj=env, custom_tags_obj=ct, **small_task_custom_cluster)


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
tables = 'com.redventures.cdm.datamart.cards.common.staging.Session'

runner_params = RunnerParams(tenants=Variable.get('DBX_CARDS_SDK_Tenants'),
                             account=ACCOUNT,
                             read_bucket='rv-core-pipeline',
                             write_bucket=Variable.get('DBX_CARDS_Bucket'),
                             paid_search_company_id=Variable.get('CARDS_PAIDSEARCH_COMPANY_IDS'),
                             environment=Variable.get('environment'),
                             tables=tables,
                             custom_parameter__dbx_secrets_scope='cards')


session_staging_jar_task = JarTask(cluster=cluster,
                                   params=runner_params,
                                   main_class="com.redventures.cdm.datamart.cards.Runner",
                                   jar_libraries=staging_libraries,
                                   tables=tables)

# DAG Creation Step
with DAG('mikes-cdmsubmit-dag',
         schedule_interval='30 8 * * *',
         dagrun_timeout=timedelta(hours=3),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    # 0. Import CdmDatabricksSubmitRunOperator, JarTask, RunnerParams
    # 1. Replace task_id with job_name
    # 2. Replace spark_jar_task with task
    # 3.
    # Optional. convert variable.get() to {{ var.value.<variable_name> }}.
    # Replace Variable.get\(\"(.*?)\"\) with \'\{\{ var.value.$1 \}\}\'

    session_staging = CdmDatabricksSubmitRunOperator(
        job_name='session-staging',
        # new_cluster=cluster,
        task=session_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
