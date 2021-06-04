#CCDC

from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from operators.finserv_operator import FinServDatabricksSubmitRunOperator
#import slack_helpers_v2 as sh
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 3),
    'email': ['vmalhotra@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),    
    'retries': 0,
    #'queue': 'data_pipeline_queue',
    'retry_delay': timedelta(minutes=5),
    #'op_kwargs': cfg_dict,
    'provide_context': True
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"

# Mapping for events
# Instant Approvals - Order Completed
# Estimated Revenue - Product Added
# App Completed     - Checkout Started
# Actual Approvals  - Order Completed

base_params = {
    "airflowDagRunID" : "{{ ti.dag_id }}" + "-" + "{{ ti.task_id }}" + "-" + "{{ ti.execution_date }}",
    "dataLakePath"    : Variable.get("DBX_DataLake_Path"),
    "logTableName"    : Variable.get("DBX_CCDC_LOG_TABLE"),
    "writeKeys"       : Variable.get("DBX_CCDC_WRITE_KEY")
}

# Cluster Setup Step
small_task_cluster = {
    'spark_version':            '5.3.x-scala2.11',
    'node_type_id':             Variable.get("DBX_MEDIUM_CLUSTER"),
    'driver_node_type_id':      Variable.get("DBX_MEDIUM_CLUSTER"),
    'num_workers':              Variable.get("DBX_MEDIUM_CLUSTER_NUM_NODES"),
    'auto_termination_minutes': 0,    
    'dbfs_cluster_log_conf':    'dbfs://home/cluster_log',
    'spark_conf': {
      'spark.sql.sources.partitionOverwriteMode': 'dynamic'
    },
    'aws_attributes': {
        'availability':             'SPOT_WITH_FALLBACK',
        'instance_profile_arn':     Variable.get("DBX_CCDC_IAM_ROLE"),
        'ebs_volume_count':         2,
        'ebs_volume_size':          100,
        'ebs_volume_type':          'GENERAL_PURPOSE_SSD',
        'first_on_demand':          '2',
        'spot_bid_price_percent':   '70',
        'zone_id':                  'us-east-1c'
    },
    'custom_tags': {
       'Partner': 'B530',
       'Project': 'CreditCards.com',
       'DagId':  "{{ti.dag_id}}",
       'TaskId': "{{ti.task_id}}"
    },
}

# Libraries 
staging_libraries = [
  {
    "jar": "dbfs:/FileStore/jars/a750569c_d6c0_425b_bf2a_a16d9f05eb25-RedshiftJDBC42_1_2_1_1001-0613f.jar",
  },
  {
    "jar": "dbfs:/data-warehouse/production/datawarehouse-builder-0.8.1-tmp.jar",
    #"jar": "dbfs:/Libraries/JVM/data-common/data-common-3.0.1-2.jar",
  },
  {
      "maven": {
        "coordinates": "org.scalaj:scalaj-http_2.11:2.4.2"
      }
  }
]

# Notebook Task Parameter Setup:
estimated_revenue_notebook_task = {
    'base_parameters': {
        "lookBackDays"             : Variable.get("DBX_CCDC_EVENTING_ESTIMATED_REVENUE_LOOKBACK_DAYS"),
        "lookBackHours"            : Variable.get("DBX_CCDC_EVENTING_ESTIMATED_REVENUE_LOOKBACK_HOURS"),    
        "loadType"                 : Variable.get("DBX_CCDC_EVENTING_ESTIMATED_REVENUE_LOAD_TYPE"),
        "toDate"                   : "now",    
        "eventName"                : "ProductAdded"
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/' + Variable.get("DBX_CCDC_CODE_ENV") + '/eventing-notebooks/conversionsProductAdded',
}

instant_approvals_notebook_task = {
    'base_parameters': {
        "lookBackDays"             : Variable.get("DBX_CCDC_EVENTING_INSTANT_APPROVALS_LOOKBACK_DAYS"),
        "lookBackHours"            : Variable.get("DBX_CCDC_EVENTING_INSTANT_APPROVALS_LOOKBACK_HOURS"),
        "loadType"                 : Variable.get("DBX_CCDC_EVENTING_INSTANT_APPROVALS_LOAD_TYPE"),
        "toDate"                   : "now",
        "eventName"                : "OrderCompleted"
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/' + Variable.get("DBX_CCDC_CODE_ENV") + '/eventing-notebooks/conversionsInstantApprovals',
}

app_completed_notebook_task = {
    'base_parameters': {
        "lookBackDays"             : Variable.get("DBX_CCDC_EVENTING_APP_COMPLETED_LOOKBACK_DAYS"),
        "lookBackHours"            : Variable.get("DBX_CCDC_EVENTING_APP_COMPLETED_LOOKBACK_HOURS"),
        "loadType"                 : Variable.get("DBX_CCDC_EVENTING_APP_COMPLETED_LOAD_TYPE"),
        "toDate"                   : "now",
        "eventName"                : "CheckoutStarted"
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/' + Variable.get("DBX_CCDC_CODE_ENV") + '/eventing-notebooks/conversionsCheckoutStarted',
}

actual_approvals_notebook_task = {
    'base_parameters': {
        "lookBackDays"             : Variable.get("DBX_CCDC_EVENTING_ACTUAL_APPROVALS_LOOKBACK_DAYS"),
        "lookBackHours"            : Variable.get("DBX_CCDC_EVENTING_ACTUAL_APPROVALS_LOOKBACK_HOURS"),
        "loadType"                 : Variable.get("DBX_CCDC_EVENTING_ACTUAL_APPROVALS_LOAD_TYPE"),
        "toDate"                   : "now",
        "eventName"                : "OrderCompleted"
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/' + Variable.get("DBX_CCDC_CODE_ENV") + '/eventing-notebooks/conversionsActualOrderCompleted',
}

delta_optimization_notebook_task = {
    'base_parameters': {
        "tableName"                : Variable.get("DBX_CCDC_LOG_TABLE")
    },
    'notebook_path': '/Production/cards-data-mart-ccdc/' + Variable.get("DBX_CCDC_CODE_ENV") + '/eventing-notebooks/optimizeDeltaTablesCCDC',
}

#updating base params
estimated_revenue_notebook_task['base_parameters'].update(base_params)
instant_approvals_notebook_task['base_parameters'].update(base_params)
app_completed_notebook_task['base_parameters'].update(base_params)
actual_approvals_notebook_task['base_parameters'].update(base_params)

# DAG Creation Step
with DAG('data-lake-dw-CCDC-fb-eventing-hourly-workflow',
          schedule_interval='15 * * * *',
          dagrun_timeout=timedelta(hours=1),
          catchup=False,
          max_active_runs=1,
          default_args=default_args) as dag:

        estimated_revenue_task = FinServDatabricksSubmitRunOperator(
            task_id                = 'estimated-revenue-to-product-added',
            new_cluster            = small_task_cluster,
            notebook_task          = estimated_revenue_notebook_task,
            libraries              = staging_libraries,
            timeout_seconds        = 3600,
            databricks_conn_id     = airflow_svc_token,
            polling_period_seconds = 120
        )

        instant_approvals_task = FinServDatabricksSubmitRunOperator(
            task_id                = 'instant-approvals-to-order-completed',
            new_cluster            = small_task_cluster,
            notebook_task          = instant_approvals_notebook_task,
            libraries              = staging_libraries,
            timeout_seconds        = 3600,
            databricks_conn_id     = airflow_svc_token,
            polling_period_seconds = 120
        )

        app_completed_task = FinServDatabricksSubmitRunOperator(
            task_id                = 'app-completed-to-checkout-started',
            new_cluster            = small_task_cluster,
            notebook_task          = app_completed_notebook_task,
            libraries              = staging_libraries,
            timeout_seconds        = 3600,
            databricks_conn_id     = airflow_svc_token,
            polling_period_seconds = 120
        )

        actual_approvals_task = FinServDatabricksSubmitRunOperator(
            task_id                = 'actual-approvals-to-order-completed',
            new_cluster            = small_task_cluster,
            notebook_task          = actual_approvals_notebook_task,
            libraries              = staging_libraries,
            timeout_seconds        = 3600,
            databricks_conn_id     = airflow_svc_token,
            polling_period_seconds = 120
        )
        
        delta_optimization_task = FinServDatabricksSubmitRunOperator(
            task_id                = 'delta-table-optimization',
            new_cluster            = small_task_cluster,
            notebook_task          = delta_optimization_notebook_task,
            libraries              = staging_libraries,
            timeout_seconds        = 3600,
            databricks_conn_id     = airflow_svc_token,
            polling_period_seconds = 120
        )
# Defining  dependencies
estimated_revenue_task  >> delta_optimization_task
instant_approvals_task  >> delta_optimization_task
app_completed_task      >> delta_optimization_task
actual_approvals_task   >> delta_optimization_task