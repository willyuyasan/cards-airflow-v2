from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from datetime import timedelta
from rvairflow import slack_hook
from rvairflow.cdm.params import RunnerParams
from rvairflow.context import CdmContext
from rvairflow.dbx.dbx_operator import CdmDatabricksSubmitRunOperator
from rvairflow.dbx.task import NewCluster, JarTask, SparkEnvVars, ClusterCustomTags, \
    DagDefaultArgs
from rvairflow.util import create_name, convert_dep_dict_2_dag_tasks_dict

# Variables
DATABRICKS_CONN_ID = 'databricks_airflow_svc_token'
ENVIRONMENT = Variable.get('CARDS_SDK_ENV')
INSTANCE_PROFILE_ARN = Variable.get('DBX_AMEX_IAM_ROLE')
SPARK_VER = Variable.get('CDM_SPARK_VERSION')
WRITE_BUCKET = Variable.get("DBX_CARDS_Bucket")
READ_BUCKET = Variable.get("CDM_READ_BUCKET")
START_DATE = (datetime.now() - (timedelta(days=int(int(Variable.get("AMEXATTRIBUTION_LOOKBACK_DAYS")))))).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")
ACCOUNT = Variable.get("CDM_ACCOUNT")
SCHEDULE_INTERVAL = Variable.get("AMEXATTRIBUTION_SCHEDULE_DAILY")

# Constants

DBX_SECRET_SCOPE = 'cards'
TENANT_ID = ['4a836525-cbc2-4923-8ee2-fba662934af9', '86ce0ab9-5128-47e1-a19b-fa6bc8799966',
             '08b9e791-59e6-45cb-9b5a-0f36db81313a', 'daf2a469-5e41-4eab-a71a-14e14cc11f58',
             '684b6ce1-dba1-4de6-92d5-761d6f3613b1', 'f91a6d8b-ad2a-403b-8814-c273dc30d885']
APP_CONF_FILE = f'application-cards-{ENVIRONMENT}'

DAG_NAME = f'amex-{ENVIRONMENT}-{ACCOUNT}-instance_attribution'
DAG_DESCRIPTION = f'amex cdm {ENVIRONMENT} instance attribution'

TRIGGER_RULE = TriggerRule.ALL_SUCCESS

ROOT_NS = 'com.redventures.cdm'
# COHESION_NS = f'{ROOT_NS}.cohesion'
# TRAFFIC_NS = f'{ROOT_NS}.trafficsources'
# FLAT_REPORTING_NS = f'{ROOT_NS}.flatreporting'
# EMAIL_NS = f'{ROOT_NS}.email'

BANKRATE_NS = f'{ROOT_NS}.cards'
# MAIN_CLASS = [
# f'{BANKRATE_NS}.Runner'
# ]

# BANKRATE_JAR_VERSION = Variable.get("BANKRATE_JAR_VERSION")
# TRAFFICSOURCES_JAR_VERSION = Variable.get("TRAFFICSOURCES_JAR_VERSION")
# COHESION_JAR_VERSION = Variable.get("COHESION_JAR_VERSION")

# Jar Libraries
JAR_LIBRARIES_DICT = {
    'cards_jar': {'jar': [

        'dbfs:/FileStore/jars/ae38779e_5fe7_405b_898f_a229470d0e88-cdm_data_mart_cards_assembly_0_0_1_SNAPSHOT-97c7e.jar'],
        'main_class': 'com.redventures.cdm.datamart.cards.Runner.main'}
}

# Create cdm context
cdm_context = CdmContext.get_instance()

# Creating CLuster
ct = ClusterCustomTags(
    cluster_type="Development",
    partner="B816",
    product="DatabricksDevelopment",
    owner="bankrate-de_svc",
    created_by='bankrate-de_svc@redventures.net',
    DagId="{{ ti.dag_id }}",
    TaskId="{{ ti.task_id }}"
)

env = SparkEnvVars(
    cdm_secret_scope='cdm_custom',
    api_secret_scope='cdm_custom',
    java_opts=f'-Dconfig.resource={APP_CONF_FILE}'
)

LOG_PATH = {
    'dbfs': {
        'destination': 'dbfs:/tmp/airflow_logs/%s/%s/%s' % (ACCOUNT, DAG_NAME, datetime.date(datetime.now()))
    }
}


def create_cluster(num_workers, node_type):
    cluster = NewCluster(
        num_workers=num_workers,
        spark_env_obj=env,
        spark_conf={'spark.driver.extraJavaOptions': f'-Dconfig.resource={APP_CONF_FILE}.conf'},
        custom_tags_obj=ct,
        cluster_log_conf=LOG_PATH,
        init_scripts=[
            {
                "dbfs": {
                    "destination": f'dbfs:/cdm/init-scripts/modify-log4j-properties-{ENVIRONMENT}.sh'
                }
            }
        ],
        instance_profile_arn=INSTANCE_PROFILE_ARN,
        spark_version=SPARK_VER,
        node_type_id=node_type,
        driver_node_type_id=node_type)
    return cluster


tables = {
    'com.redventures.cdm.datamart.cards.amex_attribution.InstanceIdAttribution': {'jar': 'cards_jar', 'cluster': create_cluster(2, 'm5a.2xlarge')

                                                                                  }
}


runner_params = RunnerParams(tenants=TENANT_ID,
                             account=ACCOUNT,
                             read_bucket=READ_BUCKET,
                             write_bucket=WRITE_BUCKET,
                             paid_search_company_id='97',
                             environment=ENVIRONMENT,
                             dbx_secret_scope='cards',
                             etl_time=str(datetime.now()),
                             start_date=START_DATE,
                             end_date=END_DATE,
                             tables=",".join([k for k, v in tables.items()])
                             )

dependencies_dict = {
    # currently no dependencies
}


jar_tasks_dict = {f'{create_name(t)}_jar_task': JarTask(cluster=j['cluster'], params=runner_params,
                                                        main_class=JAR_LIBRARIES_DICT[j['jar']]['main_class'],
                                                        jar_libraries=JAR_LIBRARIES_DICT[j['jar']]['jar'], tables=t) for
                  t, j in tables.items()}  # acl=acl
print(jar_tasks_dict)

# DAG Creation Step
# Creates DAG default arguments
default_args = DagDefaultArgs(on_failure_callback=slack_hook.slack_failure_callback(), retries=1,
                              execution_timeout=timedelta(seconds=30000)).get_dict()
dag = DAG(DAG_NAME, schedule_interval=SCHEDULE_INTERVAL, description=DAG_DESCRIPTION, dagrun_timeout=timedelta(hours=4),
          catchup=False, max_active_runs=1, default_args=default_args)
# Add tasks to the dag
dag_tasks = {}
for k, v in jar_tasks_dict.items():
    new_key = k.replace('_jar_task', '_task')
    dag_tasks[new_key] = CdmDatabricksSubmitRunOperator(job_name=new_key,
                                                        task=v,
                                                        databricks_conn_id=cd,
                                                        timeout_seconds=3600,
                                                        dag=dag,
                                                        trigger_rule=TRIGGER_RULE)

# Create the dag dependency based on the dependency table
dag_task_dep = convert_dep_dict_2_dag_tasks_dict(dependencies_dict)
for task, deps in dag_task_dep.items():
    d_t = [dag_tasks[dep] for dep in deps]
    dag_tasks[task].set_upstream(d_t)
