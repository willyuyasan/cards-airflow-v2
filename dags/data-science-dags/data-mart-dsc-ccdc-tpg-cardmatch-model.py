# Define the imports
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators import ExternalTaskSensor
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

# Define the default args
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
    'provide_context': True
}

# DBX tokens and logs
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-mart-dsc-ccdc-tpg-cardmatch-model-daily'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s/%s' % (ACCOUNT, Variable.get("log-environment"), DAG_NAME, datetime.date(datetime.now()))
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

# ETL libraries
etl_step_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/data-warehouse/production/datawarehouse-builder-0.8.1-tmp.jar",
    },
]

# model libraries
model_step_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/data-warehouse/production/datawarehouse-builder-0.8.1-tmp.jar",
    },
]

# ETL Notebook Task
etl_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "stagingPath": Variable.get("DBX_CARDS_Staging_Path")
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_new_data_pull',
}

# Model Training Notebook Task
capital_one_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Capital Bank"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

chase_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Chase"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

# Model Deployment Notebook Task
model_deployment_notebook_task = {
    'base_parameters': {
        "environment": "staging"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_combine',
}

# DAG Creation Step
with DAG('data-mart-dsc-ccdc-tpg-cardmatch-model-daily',
         schedule_interval='0 8 * * *',
         dagrun_timeout=timedelta(hours=3),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    etl_notebook_step = FinServDatabricksSubmitRunOperator(
        task_id='etl-step',
        new_cluster=small_task_cluster,
        notebook_task=etl_notebook_task,
        libraries=etl_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    capital_one_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='capital-one-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=capital_one_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    chase_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='chase-one-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=chase_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    model_deployment_step = FinServDatabricksSubmitRunOperator(
        task_id='model-deployment-step',
        new_cluster=small_task_cluster,
        notebook_task=model_deployment_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# Dependency setup
etl_notebook_step >> [capital_one_model_training_step, chase_model_training_step]
[capital_one_model_training_step, chase_model_training_step] >> model_deployment_step

