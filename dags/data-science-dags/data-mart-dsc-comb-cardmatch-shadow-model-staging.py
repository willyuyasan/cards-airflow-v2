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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'op_kwargs': cfg_dict,
    'provide_context': True
}

# DBX tokens and logs
airflow_svc_token = "databricks_airflow_svc_token"
ACCOUNT = 'cards'
DAG_NAME = 'data-mart-dsc-comb-cardmatch-shadow-model-staging'

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
        "pypi": {"package": "pandas==1.0.3"}
    },
    {
        "pypi": {"package": "matplotlib"}
    },
    {
        "pypi": {"package": "cloudpickle"}
    },
    {
        "pypi": {"package": "auth0-python"}
    },
    {
        "pypi": {"package": "numpy==1.18.4"}
    },
    {
        "pypi": {"package": "scikit-learn==0.22"}
    },
    {
        "pypi": {"package": "joblibspark"}
    }
]

# ETL Notebook Task
ccdc_etl_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "stagingPath": Variable.get("DBX_CARDS_Staging_Path")
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_new_data_pull',
}

brcc_etl_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "stagingPath": Variable.get("DBX_CARDS_Staging_Path")
    },
    'notebook_path': '/Projects/CardMatch/Combined/BRCC-CM-data',
}

tpg_etl_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "stagingPath": Variable.get("DBX_CARDS_Staging_Path")
    },
    'notebook_path': '/Projects/CardMatch/Combined/TPG-CM-data',
}

# Model Training Notebook Tasks
avant_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Avant",
        "card_ids": "6353"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

# boa_model_training_notebook_task = {
#     'base_parameters': {
#         "issuer": "Bank of America",
#         "card_ids": "220612356, 22079418, 22069416"
#     },
#     'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
# }

capital_bank_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Capital Bank",
        "card_ids": "234110088"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

chase_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Chase",
        "card_ids": "22126065, 221211283"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

citi_model_training_notebook_task_a = {
    'base_parameters': {
        "issuer": "Citi",
        "card_ids": "6295, 6379, 22146011, 221410611, 221410949"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

citi_model_training_notebook_task_b = {
    'base_parameters': {
        "issuer": "Citi",
        "card_ids": "221411361, 221411362, 22145695, 22146209, 221410118"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

credit_one_model_training_notebook_task_a = {
    'base_parameters': {
        "issuer": "Credit One",
        "card_ids": "6852, 6853, 6854, 6855, 229611108"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

credit_one_model_training_notebook_task_b = {
    'base_parameters': {
        "issuer": "Credit One",
        "card_ids": "229611306"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

credit_strong_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Credit Strong",
        "card_ids": "7482"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

discover_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Discover",
        "card_ids": "7561, 7562, 7563, 7564, 7565"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

self_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Self",
        "card_ids": "7030"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

icommissions_model_training_notebook_task_a = {
    'base_parameters': {
        "issuer": "iCommissions",
        "card_ids": "6786, 6991, 240910629, 240912393, 243212365"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

icommissions_model_training_notebook_task_b = {
    'base_parameters': {
        "issuer": "iCommissions",
        "card_ids": "7523, 7424, 22404927, 22995857, 7615"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

icommissions_model_training_notebook_task_c = {
    'base_parameters': {
        "issuer": "iCommissions",
        "card_ids": "7199, 7513, 7616"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

jasper_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Jasper",
        "card_ids": "6938"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

greenlight_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Greenlight",
        "card_ids": "7576"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

petal_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Petal",
        "card_ids": "6588, 7550"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

wells_fargo_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Wells Fargo",
        "card_ids": "7683, 7684, 7685, 7730, 7731"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow',
}

deserve_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Deserve",
        "card_ids": "249112293"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow'
}

synchrony_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Synchrony Bank",
        "card_ids": "7536"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow'
}

sofi_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "SoFi",
        "card_ids": "7700"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow'
}

premier_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "PREMIER",
        "card_ids": "22213840, 222112174, 22214990"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow'
}

mission_lane_model_training_notebook_task = {
    'base_parameters': {
        "issuer": "Mission Lane",
        "card_ids": "6968"
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_train-shadow'
}

# Model Deployment Notebook Task
model_deployment_notebook_task = {
    'base_parameters': {
        "environment": "staging",
        "notebook": '/Projects/CardMatch/Combined/CardMatch_python_combine-shadow'
    },
    'notebook_path': '/Projects/CardMatch/Combined/CardMatch_python_combine-shadow',
}

# DAG Creation Step
with DAG('data-mart-dsc-comb-cardmatch-shadow-model-staging',
         schedule_interval=None,
         dagrun_timeout=timedelta(hours=3),
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:

    ccdc_etl_notebook_step = FinServDatabricksSubmitRunOperator(
        task_id='ccdc-etl-step',
        new_cluster=small_task_cluster,
        notebook_task=ccdc_etl_notebook_task,
        libraries=etl_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    brcc_etl_notebook_step = FinServDatabricksSubmitRunOperator(
        task_id='brcc-etl-step',
        new_cluster=small_task_cluster,
        notebook_task=brcc_etl_notebook_task,
        libraries=etl_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    tpg_etl_notebook_step = FinServDatabricksSubmitRunOperator(
        task_id='tpg-etl-step',
        new_cluster=small_task_cluster,
        notebook_task=tpg_etl_notebook_task,
        libraries=etl_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    avant_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='avant-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=avant_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    # boa_model_training_step = FinServDatabricksSubmitRunOperator(
    #     task_id='BoA-model-training-step',
    #     new_cluster=small_task_cluster,
    #     notebook_task=boa_model_training_notebook_task,
    #     libraries=model_step_libraries,
    #     timeout_seconds=1800,
    #     databricks_conn_id=airflow_svc_token,
    #     polling_period_seconds=120
    # )

    capital_bank_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='capital-bank-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=capital_bank_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    chase_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='chase-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=chase_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    citi_model_training_step_a = FinServDatabricksSubmitRunOperator(
        task_id='citi-model-training-step-a',
        new_cluster=small_task_cluster,
        notebook_task=citi_model_training_notebook_task_a,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    citi_model_training_step_b = FinServDatabricksSubmitRunOperator(
        task_id='citi-model-training-step-b',
        new_cluster=small_task_cluster,
        notebook_task=citi_model_training_notebook_task_b,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    credit_one_model_training_step_a = FinServDatabricksSubmitRunOperator(
        task_id='credit-one-model-training-step-a',
        new_cluster=small_task_cluster,
        notebook_task=credit_one_model_training_notebook_task_a,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    credit_one_model_training_step_b = FinServDatabricksSubmitRunOperator(
        task_id='credit-one-model-training-step-b',
        new_cluster=small_task_cluster,
        notebook_task=credit_one_model_training_notebook_task_b,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    credit_strong_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='credit-strong-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=credit_strong_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    discover_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='discover-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=discover_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    self_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='self-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=self_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    icommissions_model_training_step_a = FinServDatabricksSubmitRunOperator(
        task_id='iCommissions-model-training-step-a',
        new_cluster=small_task_cluster,
        notebook_task=icommissions_model_training_notebook_task_a,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    icommissions_model_training_step_b = FinServDatabricksSubmitRunOperator(
        task_id='iCommissions-model-training-step-b',
        new_cluster=small_task_cluster,
        notebook_task=icommissions_model_training_notebook_task_b,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    icommissions_model_training_step_c = FinServDatabricksSubmitRunOperator(
        task_id='iCommissions-model-training-step-c',
        new_cluster=small_task_cluster,
        notebook_task=icommissions_model_training_notebook_task_c,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    jasper_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='jasper-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=jasper_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    greenlight_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='greenlight-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=greenlight_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    petal_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='Petal-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=petal_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    wells_fargo_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='Wells-Fargo-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=wells_fargo_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    deserve_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='Deserve-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=deserve_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    synchrony_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='Synchrony-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=synchrony_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    sofi_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='SoFi-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=sofi_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    premier_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='PREMIER-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=premier_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    mission_lane_model_training_step = FinServDatabricksSubmitRunOperator(
        task_id='Mission-lane-model-training-step',
        new_cluster=small_task_cluster,
        notebook_task=mission_lane_model_training_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    model_deployment_step = FinServDatabricksSubmitRunOperator(
        task_id='model-combine-deployment-step',
        new_cluster=small_task_cluster,
        notebook_task=model_deployment_notebook_task,
        libraries=model_step_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# Dependency setup
ccdc_etl_notebook_step >> [
    avant_model_training_step, capital_bank_model_training_step,
    chase_model_training_step, citi_model_training_step_a, citi_model_training_step_b,
    credit_one_model_training_step_a, credit_one_model_training_step_b, credit_strong_model_training_step,
    discover_model_training_step, self_model_training_step,
    icommissions_model_training_step_a, icommissions_model_training_step_b, icommissions_model_training_step_c,
    jasper_model_training_step, greenlight_model_training_step,
    petal_model_training_step, wells_fargo_model_training_step, deserve_model_training_step, synchrony_model_training_step,
    sofi_model_training_step, premier_model_training_step, mission_lane_model_training_step
]

brcc_etl_notebook_step >> [
    avant_model_training_step, capital_bank_model_training_step,
    chase_model_training_step, citi_model_training_step_a, citi_model_training_step_b,
    credit_one_model_training_step_a, credit_one_model_training_step_b, credit_strong_model_training_step,
    discover_model_training_step, self_model_training_step,
    icommissions_model_training_step_a, icommissions_model_training_step_b, icommissions_model_training_step_c,
    jasper_model_training_step, greenlight_model_training_step,
    petal_model_training_step, wells_fargo_model_training_step, deserve_model_training_step, synchrony_model_training_step,
    sofi_model_training_step, premier_model_training_step, mission_lane_model_training_step
]

tpg_etl_notebook_step >> [
    avant_model_training_step, capital_bank_model_training_step,
    chase_model_training_step, citi_model_training_step_a, citi_model_training_step_b,
    credit_one_model_training_step_a, credit_one_model_training_step_b, credit_strong_model_training_step,
    discover_model_training_step, self_model_training_step,
    icommissions_model_training_step_a, icommissions_model_training_step_b, icommissions_model_training_step_c,
    jasper_model_training_step, greenlight_model_training_step,
    petal_model_training_step, wells_fargo_model_training_step, deserve_model_training_step, synchrony_model_training_step,
    sofi_model_training_step, premier_model_training_step, mission_lane_model_training_step
]

[avant_model_training_step, capital_bank_model_training_step,
 chase_model_training_step, citi_model_training_step_a, citi_model_training_step_b,
 credit_one_model_training_step_a, credit_one_model_training_step_b, credit_strong_model_training_step,
 discover_model_training_step, self_model_training_step,
 icommissions_model_training_step_a, icommissions_model_training_step_b, icommissions_model_training_step_c,
 jasper_model_training_step, greenlight_model_training_step,
 petal_model_training_step, wells_fargo_model_training_step, deserve_model_training_step, synchrony_model_training_step,
 sofi_model_training_step, premier_model_training_step, mission_lane_model_training_step
 ] >> model_deployment_step
