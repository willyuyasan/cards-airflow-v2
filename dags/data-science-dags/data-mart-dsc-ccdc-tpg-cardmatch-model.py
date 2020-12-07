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
    'start_date': datetime(2020, 11, 1),
    'email': ['gbennett@redventures.com'],
    'email_on_failure': True,
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
DAG_NAME = 'data-mart-dsc-ccdc-tpg-cardmatch-model-monthly'

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s/%s' % (ACCOUNT, Variable.get("log-environment"), DAG_NAME, datetime.date(datetime.now()))
    }
}

# Cluster Setup Step
small_task_cluster = {
    'spark_version': '6.6.x-scala2.11',
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
    {
        "jar": "dbfs:/FileStore/jars/767dc197_7942_43d9_87fe_69b7349e03ab-data_common_2_4_3-b2fba.jar"
    }
]

# model libraries
model_step_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/data-warehouse/production/datawarehouse-builder-0.8.1-tmp.jar",
    },
    {
        "jar": "dbfs:/FileStore/jars/767dc197_7942_43d9_87fe_69b7349e03ab-data_common_2_4_3-b2fba.jar"
    },
    {
        "whl": "dbfs:/Libraries/Python/python-client/dradis_client-0.8-py3-none-any.whl"
    },
    {
        "pypi": "pandas==1.0.3"
    },
    {
        "pypi": "matplotlib"
    },
    {
        "pypi": "cloudpickle"
    },
    {
        "pypi": "auth0-python"
    },
    {
        "pypi": "numpy==1.18.4"
    },
    {
        "pypi": "scikit-learn==0.22"
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

# Model Training Notebook Tasks
avant_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Avant"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

boa_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Bank of America"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

capital_bank_model_training_notebook_task = {
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

citi_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Citi"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

credit_one_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Credit One"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

credit_strong_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Credit Strong"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

discover_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Discover"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

self_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Self"
    },
    'notebook_path': '/Projects/CardMatch/Airflow-capable/CardMatch_python_train',
}

icommissions_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "iCommissions"
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
         schedule_interval=None,
         dagrun_timeout=timedelta(hours=2),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    etl_notebook_step = FinServDatabricksSubmitRunOperator(
        task_id='etl-step',
        new_cluster=small_task_cluster,
        notebook_task=etl_notebook_task,
        libraries=etl_step_libraries,
        timeout_seconds=1800,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    avant_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='avant-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=avant_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    boa_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='BoA-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=boa_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    capital_bank_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='capital-bank-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=capital_bank_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    chase_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='chase-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=chase_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    citi_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='citi-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=citi_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    credit_one_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='credit-one-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=credit_one_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    credit_strong_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='credit-strong-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=credit_strong_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    discover_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='discover-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=discover_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    self_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='self-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=self_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    icommissions_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='iCommissions-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=icommissions_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    model_deployment_step = FinServDatabricksSubmitRunOperator(
        task_id='model-combine-deployment-step',
        new_cluster=small_task_cluster,
        notebook_task=model_deployment_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# Dependency setup
etl_notebook_step >> [avant_model_training_step, boa_model_training_step, capital_bank_model_training_step,
                      chase_model_training_step, citi_model_training_step, credit_one_model_training_step,
                      credit_strong_model_training_step, discover_model_training_step, self_model_training_step,
                      icommissions_model_training_step]
[avant_model_training_step, boa_model_training_step, capital_bank_model_training_step,
 chase_model_training_step, citi_model_training_step, credit_one_model_training_step,
 credit_strong_model_training_step, discover_model_training_step, self_model_training_step,
 icommissions_model_training_step] >> model_deployment_step
