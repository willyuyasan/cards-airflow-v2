# Migrating Legacy to New Airflow 6/7/2021
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
from airflow.operators.python_operator import BranchPythonOperator

from rvairflow import slack_hook as sh

default_args = {  # 'op_kwargs': cfg_dict,
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 19),
    'email': ['vmalhotra@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'cluster_permissions': Variable.get("DE_DBX_CLUSTER_PERMISSIONS")
}

# token variable

airflow_svc_token = 'databricks_airflow_svc_token'

# Cluster Setup Step

small_i3_x_1w_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_SMALL_CLUSTER"),
    'num_workers': Variable.get("DBX_SMALL_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf': 'dbfs://home/cluster_log',
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'aws_attributes': {
        'ebs_volume_count': 2,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        'availability': 'SPOT_WITH_FALLBACK',
        'instance_profile_arn': Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': ' B532', 'Project': 'The Points Guy'
    },
}

small_i3_x_3w_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': Variable.get("DBX_MEDIUM_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf': 'dbfs://home/cluster_log',
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'aws_attributes': {
        'ebs_volume_count': 3,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        'availability': 'SPOT_WITH_FALLBACK',
        'instance_profile_arn': Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': ' B532',
        'Project': 'The Points Guy'
    },
}

small_r4_2x_2w_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': Variable.get("DBX_MEDIUM_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf': 'dbfs://home/cluster_log',
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'aws_attributes': {
        'ebs_volume_count': 3,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        'availability': 'SPOT_WITH_FALLBACK',
        'instance_profile_arn': Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': ' B532',
        'Project': 'The Points Guy'
    },
}

small_r4_2x_4w_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': Variable.get("DBX_MEDIUM_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf': 'dbfs://home/cluster_log',
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'aws_attributes': {
        'ebs_volume_count': 4,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '70',
        'zone_id': 'us-east-1c',
        'availability': 'SPOT_WITH_FALLBACK',
        'instance_profile_arn': Variable.get("DBX_TPG_IAM_ROLE"),
    },
    'custom_tags': {
        'Partner': ' B532',
        'Project': 'The Points Guy'
    },
}

# Libraries
notebook_libraries = [
    {
        "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
    },
    {
        "jar": "dbfs:/Libraries/JVM/data-common/data-common-3.0.1-2.jar",
    },
    {
        "maven": {
            "coordinates": "org.scalaj:scalaj-http_2.12:2.4.2"
        }
    }
]

# Notebook Task Parameter Setup:
decision_notebook_task = {
    'base_parameters': {
        "updateRedshiftSchema": Variable.get("DBX_TPG_Adzerk_Redshift_Schema_Update"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Adzerk_Redshift_Environment"),
        "lookBackDays": Variable.get("DBX_TPG_Adzerk_Decision_Lookbackdays"),
        "toDate": Variable.get("DBX_TPG_Adzerk_Decision_EndDate"),
    },
    'notebook_path': '/Production/cards-data-mart-tpg/' + Variable.get("DBX_TPG_CODE_ENV") + '/tpg-adzerk/Decision',
}
request_notebook_task = {
    'base_parameters': {
        "updateRedshiftSchema": Variable.get("DBX_TPG_Adzerk_Redshift_Schema_Update"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Adzerk_Redshift_Environment"),
        "lookBackDays": Variable.get("DBX_TPG_Adzerk_Requests_Lookbackdays"),
        "toDate": Variable.get("DBX_TPG_Adzerk_Request_EndDate"),
    },
    'notebook_path': '/Production/cards-data-mart-tpg/' + Variable.get("DBX_TPG_CODE_ENV") + '/tpg-adzerk/Requests',
}
selection_notebook_task = {
    'base_parameters': {
        "updateRedshiftSchema": Variable.get("DBX_TPG_Adzerk_Redshift_Schema_Update"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Adzerk_Redshift_Environment"),
        "lookBackDays": Variable.get("DBX_TPG_Adzerk_Requests_Lookbackdays"),
        "toDate": Variable.get("DBX_TPG_Adzerk_Request_EndDate"),
    },
    'notebook_path': '/Production/cards-data-mart-tpg/' + Variable.get("DBX_TPG_CODE_ENV") + '/tpg-adzerk/Selections',
}
dimensions_table_notebook_task = {
    'base_parameters': {
        "redshiftEnvironment": Variable.get("DBX_TPG_Adzerk_Redshift_Environment"),
    },
    'notebook_path': '/Production/cards-data-mart-tpg/' + Variable.get("DBX_TPG_CODE_ENV") + '/tpg-adzerk/adzerk-dim-tables',
}

# Adzerk Notebook Task Parameter Setup:
adzerk_impressions_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "eventName": "impression",
        "lookBackDays": Variable.get("DBX_TPG_Adzerk_Impressions_Lookbackdays"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Adzerk_Redshift_Environment"),
    },
    'notebook_path': '/Production/cards-data-mart-tpg/' + Variable.get("DBX_TPG_CODE_ENV") + '/tpg-adzerk/impressions',
}

# Adzerk queued reports
adzerk_queued_report_notebook_task = {
    'base_parameters': {
        "toDate": "now",
        "lookBackDays": Variable.get("DBX_TPG_Adzerk_Queued_Lookbackdays"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Adzerk_Redshift_Environment"),
    },
    'notebook_path': '/Production/cards-data-mart-tpg/' + Variable.get("DBX_TPG_CODE_ENV") + '/tpg-adzerk/Adzerk-Queued-Reports',
}

# DAG Creation Step

with DAG('adzerk-dw-tpg-adzerk-hourly-workflow',
         schedule_interval='0 * * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    # Dimension Tables
    adzerk_dimension_tables_task = FinServDatabricksSubmitRunOperator(
        task_id='raw-dimension-tables',
        new_cluster=small_i3_x_3w_task_cluster,
        notebook_task=dimensions_table_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240,
    )

    # Decision Events
    adzerk_decision_task = FinServDatabricksSubmitRunOperator(
        task_id='decision-events-to-redshift',
        new_cluster=small_r4_2x_2w_task_cluster,
        notebook_task=decision_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240,
    )

    # Request Events
    adzerk_request_task = FinServDatabricksSubmitRunOperator(
        task_id='request-events-to-redshift',
        new_cluster=small_r4_2x_2w_task_cluster,
        notebook_task=request_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240,
    )
    # Selection Events
    adzerk_selection_task = FinServDatabricksSubmitRunOperator(
        task_id='selection-events-to-redshift',
        new_cluster=small_r4_2x_2w_task_cluster,
        notebook_task=selection_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240,
    )
    # Adzerk queued report

    adzerk_queued_task = FinServDatabricksSubmitRunOperator(
        task_id='queued-reports',
        new_cluster=small_r4_2x_2w_task_cluster,
        notebook_task=adzerk_queued_report_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240,
    )

    # Impressions Events

    adzerk_impressions_data_task = FinServDatabricksSubmitRunOperator(
        task_id='impressions-events-to-redshift',
        new_cluster=small_r4_2x_4w_task_cluster,
        notebook_task=adzerk_impressions_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# Dependencies

adzerk_dimension_tables_task >> adzerk_decision_task
adzerk_dimension_tables_task >> adzerk_request_task
adzerk_dimension_tables_task >> adzerk_impressions_data_task
adzerk_dimension_tables_task >> adzerk_selection_task

adzerk_dimension_tables_task >> adzerk_queued_task
