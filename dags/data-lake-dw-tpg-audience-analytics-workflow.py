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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 1),
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
airflow_svc_token = "databricks_airflow_svc_token"

# Cluster Setup Step
large_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
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
        'Partner': ' B532',
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

medium_2w_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': 2,
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
        'Partner': ' B532',
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

medium_4w_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers': 4,
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
        'Partner': ' B532',
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
    },
}

large_4w_task_cluster = {
    'spark_version': '7.6.x-scala2.12',
    'node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'driver_node_type_id': Variable.get("DBX_LARGE_CLUSTER"),
    'num_workers': Variable.get("DBX_LARGE_CLUSTER_NUM_NODES"),
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
        'Project': 'The Points Guy',
        'DagId': "{{ti.dag_id}}",
        'TaskId': "{{ti.task_id}}"
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
        "pypi": {
            "package": "datarobot",
        }
    },
    {
        "pypi": {
            "package": "attr",
        }
    }
]

# Notebook Task Parameter Setup:
session_flow_raw_sessions_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "windowStartDate": (datetime.now() - timedelta(days=60)).date().strftime('%Y-%m-%d'),
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "raw_session_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_sessions",
        "publish_date_info_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/publish_date",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")},
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/session-flow-noteboooks/Raw Sessions',
}

# (datetime.now() - timedelta(days=60)).date().strftime('%Y-%m-%d')
# datetime.now().date().strftime('%Y-%m-%d')

session_flow_session_means_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "fromDate": Variable.get("DBX_TPG_Audience_Analytics_MeanStartDate"),
        "toDate": Variable.get("DBX_TPG_Audience_Analytics_MeanEndDate"),
        "maxAnonSize": "250",
        "raw_session_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_sessions",
        "session_means_raw_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_means_raw",
        "session_means_clean_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_means_clean",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/session-flow-noteboooks/Session Means',
}

session_flow_score_sessions_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "windowStartDate": (datetime.now() - timedelta(days=60)).date().strftime('%Y-%m-%d'),
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "raw_session_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_sessions",
        "session_means_raw_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_means_raw",
        "audience_session_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_table",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/session-flow-noteboooks/Score Sessions',
}

session_flow_write_sessions_to_redshift_notebook_task = {
    'base_parameters': {
        "fromDate": (datetime.now() - timedelta(days=60)).date().strftime('%Y-%m-%d'),
        "toDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_session_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_table",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Audience_Analytics_redshiftEnvironment"),
        "maxLoadIncrement": Variable.get("DBX_TPG_Audience_Analytics_maxLoadIncrement"),
        "updateSchema": Variable.get("DBX_TPG_Audience_Analytics_updateSchema_sessions")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/session-flow-noteboooks/Write Sessions to Redshift',
}

core_url_flow_raw_url_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "windowStartDate": "2018-07-01",
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "raw_session_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_sessions",
        "raw_url_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_url",
        "audience_session_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_table",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-url-flow-notebooks/Raw URL',
}

core_url_flow_url_means_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "fromDate": Variable.get("DBX_TPG_Audience_Analytics_MeanStartDate"),
        "toDate": Variable.get("DBX_TPG_Audience_Analytics_MeanEndDate"),
        "maxAnonSize": "250",
        "raw_url_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_url",
        "url_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/url_means",
        "raw_publish_date_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_publish_date",
        "publish_date_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/publish_date_means"
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-url-flow-notebooks/URL Means',
}

core_url_flow_score_url_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "windowStartDate": "2018-07-01",
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "url_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/url_means",
        "raw_url_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_url",
        "finalURL_temp_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/finalURL_temp",
        "audience_url_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/url_table",
        "content_predictions_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/content_predictions",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-url-flow-notebooks/Score URL',
}

core_url_flow_content_score_predictions_notebook_task = {
    'base_parameters': {
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_url_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/url_table",
        "content_predictions_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/content_predictions",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-url-flow-notebooks/Content Score Predictions',
}
core_url_flow_score_url_table_update_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "windowStartDate": "2018-07-01",
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "url_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/url_means",
        "raw_url_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_url",
        "finalURL_temp_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/finalURL_temp",
        "audience_url_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/url_table",
        "content_predictions_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/content_predictions",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-url-flow-notebooks/Score URL Table Update',
}

core_url_flow_write_url_to_redshift_notebook_task = {
    'base_parameters': {
        "fromDate": "2018-07-01",
        "toDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audienceScoringByUrlPath": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/url_table",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Audience_Analytics_redshiftEnvironment"),
        "updateSchema": Variable.get("DBX_TPG_Audience_Analytics_updateSchema_url")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-url-flow-notebooks/Write URL to Redshift',
}
core_campaign_flow_raw_adid_notebook_task = {
    'base_parameters': {
        "windowStartDate": "2018-07-01",
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_session_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_table",
        "raw_adid_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_adid",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Raw Adid',
}

core_campaign_flow_raw_adset_notebook_task = {
    'base_parameters': {
        "windowStartDate": "2018-07-01",
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_session_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_table",
        "raw_adset_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_adset",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Raw Adset',
}
core_campaign_flow_raw_postid_notebook_task = {
    'base_parameters': {
        "windowStartDate": "2018-07-01",
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_session_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/session_table",
        "raw_postid_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_postid",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Raw PostID',
}
core_campaign_flow_adid_means_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "fromDate": Variable.get("DBX_TPG_Audience_Analytics_MeanStartDate"),
        "toDate": Variable.get("DBX_TPG_Audience_Analytics_MeanEndDate"),
        "maxAnonSize": "250",
        "raw_adid_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_adid",
        "adid_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/adid_means"
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Adid Means',
}
core_campaign_flow_adset_means_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "fromDate": Variable.get("DBX_TPG_Audience_Analytics_MeanStartDate"),
        "toDate": Variable.get("DBX_TPG_Audience_Analytics_MeanEndDate"),
        "maxAnonSize": "250",
        "raw_adset_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_adset",
        "adset_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/adset_means"
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Adset Means',
}
core_campaign_flow_postid_means_notebook_task = {
    'base_parameters': {
        "dataLakePath": Variable.get("DBX_DataLake_Path"),
        "stagingPath": Variable.get("DBX_TPG_Staging_Path"),
        "reportingPath": Variable.get("DBX_TPG_Reporting_Path"),
        "fromDate": Variable.get("DBX_TPG_Audience_Analytics_MeanStartDate"),
        "toDate": Variable.get("DBX_TPG_Audience_Analytics_MeanEndDate"),
        "maxAnonSize": "250",
        "raw_postid_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_postid",
        "postid_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/postid_means"
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/PostID Means',
}
core_campaign_flow_score_adid_notebook_task = {
    'base_parameters': {
        "windowStartDate": (datetime.now() - timedelta(days=60)).date().strftime('%Y-%m-%d'),
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_adid_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/adid_table",
        "raw_adid_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_adid",
        "adid_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/adid_means"
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Score Adid',
}
core_campaign_flow_score_adset_notebook_task = {
    'base_parameters': {
        "windowStartDate": (datetime.now() - timedelta(days=60)).date().strftime('%Y-%m-%d'),
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_adset_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/adset_table",
        "raw_adset_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_adset",
        "adset_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/adset_means"
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Score Adset',
}
core_campaign_flow_score_postid_notebook_task = {
    'base_parameters': {
        "windowStartDate": (datetime.now() - timedelta(days=60)).date().strftime('%Y-%m-%d'),
        "windowEndDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_postid_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/postid_table",
        "raw_postid_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/raw_postid",
        "postid_means_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/postid_means"
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Score PostID',
}

core_campaign_flow_write_campaign_to_redshift_notebook_task = {
    'base_parameters': {
        "fromDate": "2018-07-01",
        "toDate": datetime.now().date().strftime('%Y-%m-%d'),
        "maxAnonSize": "250",
        "audience_postid_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/postid_table",
        "audience_adset_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/adset_table",
        "audience_adid_table_path": Variable.get("DBX_TPG_Audience_Analytics_Environment") + "/adid_table",
        "baseEnvironment": Variable.get("DBX_TPG_Audience_Analytics_Environment"),
        "redshiftEnvironment": Variable.get("DBX_TPG_Audience_Analytics_redshiftEnvironment"),
        "updateSchema": Variable.get("DBX_TPG_Audience_Analytics_updateSchema_campaign")
    },
    'notebook_path': '/Production/cards-data-analytics-tpg-audience/' + Variable.get("DBX_TPG_AUDIENCE_CODE_ENV") + '/core-campaign-flow-notebooks/Write Campaign to Redshift',
}


def check_session_flow_sessions_mean(**kwargs):
    if Variable.get("DBX_TPG_Audience_Analytics_Run_Sessions_Means") == 'True':
        return 'session-flow-session-means'
    else:
        return 'continue-without-sessions-means'


def check_core_url_flow_url_means(**kwargs):
    if Variable.get("DBX_TPG_Audience_Analytics_Run_Url_Means") == 'True':
        return 'core-url-flow-url-means'
    else:
        return 'continue-without-url-means'


def check_core_campaign_flow_adid_means(**kwargs):
    if Variable.get("DBX_TPG_Audience_Analytics_Run_Adid_Means") == 'True':
        return 'core-campaign-flow-adid-means'
    else:
        return 'continue-without-adid-means'


def check_core_campaign_flow_adset_means(**kwargs):
    if Variable.get("DBX_TPG_Audience_Analytics_Run_Adset_Means") == 'True':
        return 'core-campaign-flow-adset-means'
    else:
        return 'continue-without-adset-means'


def check_core_campaign_flow_postid_means(**kwargs):
    if Variable.get("DBX_TPG_Audience_Analytics_Run_Postid_Means") == 'True':
        return 'core-campaign-flow-postid-means'
    else:
        return 'continue-without-postid-means'


# DAG Creation Step
with DAG('data-lake-dw-tpg-audience-analytics-workflow',
         schedule_interval='30 8 * * *',
         dagrun_timeout=timedelta(hours=6),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    # Session Flow Workflow
    session_flow_raw_sessions = FinServDatabricksSubmitRunOperator(
        task_id='session-flow-raw-sessions',
        new_cluster=medium_4w_task_cluster,
        notebook_task=session_flow_raw_sessions_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=7560,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=240
    )

    sessions_flow_check_sessions_mean_run_variable = BranchPythonOperator(
        task_id='check-sessions-mean-run-variable',
        provide_context=True,
        python_callable=check_session_flow_sessions_mean
    )

    sessions_flow_continue_without_sessions_means = DummyOperator(
        task_id='continue-without-sessions-means'
    )

    session_flow_session_means = FinServDatabricksSubmitRunOperator(
        task_id='session-flow-session-means',
        new_cluster=medium_2w_task_cluster,
        notebook_task=session_flow_session_means_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    session_flow_score_sessions = FinServDatabricksSubmitRunOperator(
        task_id='session-flow-score-sessions',
        new_cluster=medium_2w_task_cluster,
        notebook_task=session_flow_score_sessions_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3000,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120,
        trigger_rule='one_success'
    )

    session_flow_write_sessions_to_redshift = FinServDatabricksSubmitRunOperator(
        task_id='session-flow-write-sessions-to-redshift',
        new_cluster=large_4w_task_cluster,
        notebook_task=session_flow_write_sessions_to_redshift_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=8400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120,
    )
    # Core URL Flow Workflow
    core_url_flow_raw_url = FinServDatabricksSubmitRunOperator(
        task_id='core-url-flow-raw-url',
        new_cluster=medium_4w_task_cluster,
        notebook_task=core_url_flow_raw_url_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=3000,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    core_url_flow_check_url_mean_run_variable = BranchPythonOperator(
        task_id='check-core-url-mean-run-variable',
        provide_context=True,
        python_callable=check_core_url_flow_url_means
    )

    core_url_flow_continue_without_url_means = DummyOperator(
        task_id='continue-without-url-means'
    )

    core_url_flow_url_means = FinServDatabricksSubmitRunOperator(
        task_id='core-url-flow-url-means',
        new_cluster=large_task_cluster,
        notebook_task=core_url_flow_url_means_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=7560,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    core_url_flow_score_url = FinServDatabricksSubmitRunOperator(
        task_id='core-url-flow-score-url',
        new_cluster=medium_4w_task_cluster,
        notebook_task=core_url_flow_score_url_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120,
        trigger_rule='one_success'
    )
    core_url_flow_content_score_predictions = FinServDatabricksSubmitRunOperator(
        task_id='core-url-flow-content-score-predictions',
        new_cluster=medium_2w_task_cluster,
        notebook_task=core_url_flow_content_score_predictions_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    core_url_flow_score_url_table_update = FinServDatabricksSubmitRunOperator(
        task_id='core-url-flow-score-url-table-update',
        new_cluster=large_task_cluster,
        notebook_task=core_url_flow_score_url_table_update_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    core_url_flow_write_url_to_redhsift = FinServDatabricksSubmitRunOperator(
        task_id='core-url-flow-write-url-to-redshift',
        new_cluster=medium_2w_task_cluster,
        notebook_task=core_url_flow_write_url_to_redshift_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )
    # Core Campaign Flow Workflow
    core_campaign_flow_raw_adid = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-raw-adid',
        new_cluster=large_4w_task_cluster,
        notebook_task=core_campaign_flow_raw_adid_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=6400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    core_campaign_flow_raw_adset = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-raw-adset',
        new_cluster=large_4w_task_cluster,
        notebook_task=core_campaign_flow_raw_adset_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=6400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    core_campaign_flow_raw_postid = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-raw-postid',
        new_cluster=large_4w_task_cluster,
        notebook_task=core_campaign_flow_raw_postid_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=6400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    # Check for ADID means
    core_campaign_flow_check_adid_mean_run_variable = BranchPythonOperator(
        task_id='check-core-campaign-mean-adid-run-variable',
        provide_context=True,
        python_callable=check_core_campaign_flow_adid_means
    )

    core_campaign_flow_continue_without_adid_means = DummyOperator(
        task_id='continue-without-adid-means'
    )

    core_campaign_flow_adid_means = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-adid-means',
        new_cluster=large_task_cluster,
        notebook_task=core_campaign_flow_adid_means_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    # Check for ADSET means
    core_campaign_flow_check_adset_mean_run_variable = BranchPythonOperator(
        task_id='check-core-campaign-mean-adset-run-variable',
        provide_context=True,
        python_callable=check_core_campaign_flow_adset_means
    )

    core_campaign_flow_continue_without_adset_means = DummyOperator(
        task_id='continue-without-adset-means'
    )

    core_campaign_flow_adset_means = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-adset-means',
        new_cluster=large_task_cluster,
        notebook_task=core_campaign_flow_adset_means_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    # Check for postID means
    core_campaign_flow_check_postid_mean_run_variable = BranchPythonOperator(
        task_id='check-core-campaign-mean-postid-run-variable',
        provide_context=True,
        python_callable=check_core_campaign_flow_postid_means
    )

    core_campaign_flow_continue_without_postid_means = DummyOperator(
        task_id='continue-without-postid-means'
    )

    core_campaign_flow_postid_means = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-postid-means',
        new_cluster=large_task_cluster,
        notebook_task=core_campaign_flow_postid_means_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=1200,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    # Run Score Means
    core_campaign_flow_score_adid = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-score-adid',
        new_cluster=large_task_cluster,
        notebook_task=core_campaign_flow_score_adid_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=660,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120,
        trigger_rule='one_success'
    )

    core_campaign_flow_score_adset = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-score-adset',
        new_cluster=large_task_cluster,
        notebook_task=core_campaign_flow_score_adset_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=7560,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120,
        trigger_rule='one_success'
    )

    core_campaign_flow_score_postid = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-score-postid',
        new_cluster=large_task_cluster,
        notebook_task=core_campaign_flow_score_postid_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=7560,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120,
        trigger_rule='one_success'
    )

    core_campaign_flow_write_campaign_to_redshift = FinServDatabricksSubmitRunOperator(
        task_id='core-campaign-flow-write-campaign-to-redshift',
        new_cluster=medium_2w_task_cluster,
        notebook_task=core_campaign_flow_write_campaign_to_redshift_notebook_task,
        libraries=notebook_libraries,
        timeout_seconds=2400,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

# Session Flow dependencies
session_flow_raw_sessions >> sessions_flow_check_sessions_mean_run_variable >> [sessions_flow_continue_without_sessions_means, session_flow_session_means]
sessions_flow_continue_without_sessions_means >> session_flow_score_sessions
session_flow_session_means >> session_flow_score_sessions
session_flow_score_sessions >> session_flow_write_sessions_to_redshift

# Core URL Flow dependencies
session_flow_score_sessions >> core_url_flow_raw_url
core_url_flow_raw_url >> core_url_flow_check_url_mean_run_variable >> [core_url_flow_continue_without_url_means, core_url_flow_url_means]
core_url_flow_continue_without_url_means >> core_url_flow_score_url
core_url_flow_url_means >> core_url_flow_score_url
core_url_flow_score_url >> core_url_flow_content_score_predictions
core_url_flow_content_score_predictions >> core_url_flow_score_url_table_update
core_url_flow_score_url_table_update >> core_url_flow_write_url_to_redhsift

# Core Campaign Flow dependencies
session_flow_score_sessions >> core_campaign_flow_raw_adid
core_campaign_flow_raw_adid >> core_campaign_flow_check_adid_mean_run_variable >> [core_campaign_flow_continue_without_adid_means, core_campaign_flow_adid_means]
core_campaign_flow_continue_without_adid_means >> core_campaign_flow_score_adid
core_campaign_flow_adid_means >> core_campaign_flow_score_adid

session_flow_score_sessions >> core_campaign_flow_raw_adset
core_campaign_flow_raw_adset >> core_campaign_flow_check_adset_mean_run_variable >> [core_campaign_flow_continue_without_adset_means, core_campaign_flow_adset_means]
core_campaign_flow_continue_without_adset_means >> core_campaign_flow_score_adset
core_campaign_flow_adset_means >> core_campaign_flow_score_adset


session_flow_score_sessions >> core_campaign_flow_raw_postid
core_campaign_flow_raw_postid >> core_campaign_flow_check_postid_mean_run_variable >> [core_campaign_flow_continue_without_postid_means, core_campaign_flow_postid_means]
core_campaign_flow_continue_without_postid_means >> core_campaign_flow_score_postid
core_campaign_flow_postid_means >> core_campaign_flow_score_postid

[core_campaign_flow_score_postid, core_campaign_flow_score_adset, core_campaign_flow_score_adid] >> core_campaign_flow_write_campaign_to_redshift
