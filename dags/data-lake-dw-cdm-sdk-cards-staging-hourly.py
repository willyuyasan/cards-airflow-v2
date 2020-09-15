from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from rvairflow import slack_hook as sh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email': ['vmalhotra@redventures.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name")),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'op_kwargs': cfg_dict,
    'provide_context': True
}

# token variable
airflow_svc_token = "databricks_airflow_svc_token"

# Cluster Setup Step
extra_small_task_custom_cluster = {
    'spark_version': '5.3.x-scala2.11',
    'node_type_id': 'm5a.xlarge',
    'driver_node_type_id': 'm5a.xlarge',
    'num_workers': 1,
    'auto_termination_minutes': 0,
    'dbfs_cluster_log_conf': 'dbfs://home/cluster_log',
    'spark_conf': {
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.driver.extraJavaOptions': '-Dconfig.resource=application-cards-qa.conf',
        'spark.databricks.clusterUsageTags.autoTerminationMinutes': '60'
    },
    'spark_env_vars': {
        'java_opts': '-Dconfig.resource=application-cards-qa.conf'
    },
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        'ebs_volume_count': 1,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': '2',
        'spot_bid_price_percent': '60',
        'zone_id': 'us-east-1c',
        "instance_profile_arn": Variable.get("DBX_CCDC_IAM_ROLE"),
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

# Notebook Task Parameter Setup:
session_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.Session",
        "lookBackDays=" + "10",
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

page_view_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.PageView",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

cookie_identified_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.CookieIdentified",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

field_inputted_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FieldsInputted",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

location_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.Location",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

device_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.Device",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

decision_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.Decision",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

traffic_sources_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.trafficsources.staging.TrafficSources",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

page_metrics_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.PageMetrics",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

form_submitted_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.FormSubmitted",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_CCDC_AMEX_BUSINESS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

element_viewed_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ElementViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

element_clicked_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ElementClicked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

product_clicked_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ProductClicked",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

product_viewed_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.ProductViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

amp_page_viewed_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime("%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.cohesion.staging.AmpPageViewed",
        "ACCOUNT=" + "cards",
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_TPG_Tenant_Id"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

paidsearch_staging_jar_task = {
    'main_class_name': "com.redventures.cdm.datamart.cards.Runner",
    'parameters': [
        "RUN_FREQUENCY=" + "hourly",
        "START_DATE=" + (
            datetime.now() - (timedelta(days=int(int(Variable.get("DBX_CCDC_SDK_lookback_days")))))).strftime(
            "%Y-%m-%d"),
        "END_DATE=" + datetime.now().strftime("%Y-%m-%d"),
        "TABLES=" + "com.redventures.cdm.datamart.cards.common.staging.PaidSearch",
        "ACCOUNT=" + "cards",
        "PAID_SEARCH_COMPANY_ID=" + Variable.get("CARDS_PAIDSEARCH_COMPANY_IDS"),
        "READ_BUCKET=" + "rv-core-pipeline",
        "TENANTS=" + Variable.get("DBX_CARDS_SDK_Tenants"),
        "WRITE_BUCKET=" + "rv-core-ccdc-datamart-qa"
    ]
}

# DAG Creation Step
with DAG('data-lake-dw-cdm-sdk-cards-staging-hourly',
         schedule_interval='30 0-5,9-23 * * *',
         dagrun_timeout=timedelta(hours=1),
         catchup=False,
         max_active_runs=1,
         default_args=default_args) as dag:

    session_staging = DatabricksSubmitRunOperator(
        task_id='session-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=session_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    traffic_sources_staging = DatabricksSubmitRunOperator(
        task_id='traffic-sources-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=traffic_sources_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    page_view_staging = DatabricksSubmitRunOperator(
        task_id='page-view-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=page_view_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    cookie_identified_staging = DatabricksSubmitRunOperator(
        task_id='cookie-identified-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=cookie_identified_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    field_inputted_staging = DatabricksSubmitRunOperator(
        task_id='field-inputted-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=field_inputted_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    location_staging = DatabricksSubmitRunOperator(
        task_id='location-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=location_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    device_staging = DatabricksSubmitRunOperator(
        task_id='device-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=device_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    decsion_staging = DatabricksSubmitRunOperator(
        task_id='decision-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=decision_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    page_metrics_staging = DatabricksSubmitRunOperator(
        task_id='page-metrics-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=page_metrics_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    form_submitted_staging = DatabricksSubmitRunOperator(
        task_id='form-submitted-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=form_submitted_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_clicked_staging = DatabricksSubmitRunOperator(
        task_id='product-clicked-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=product_clicked_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    product_viewed_staging = DatabricksSubmitRunOperator(
        task_id='product-viewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=product_viewed_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    element_clicked_staging = DatabricksSubmitRunOperator(
        task_id='element-clicked-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=element_clicked_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    element_viewed_staging = DatabricksSubmitRunOperator(
        task_id='element-viewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=element_viewed_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    amp_page_viewed_staging = DatabricksSubmitRunOperator(
        task_id='amp-page-viewed-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=amp_page_viewed_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    paidsearch_staging = DatabricksSubmitRunOperator(
        task_id='paidsearch-staging',
        new_cluster=extra_small_task_custom_cluster,
        spark_jar_task=paidsearch_staging_jar_task,
        libraries=staging_libraries,
        timeout_seconds=3600,
        databricks_conn_id=airflow_svc_token,
        polling_period_seconds=120
    )

    ccdc_staging_tables = DummyOperator(
        task_id='external-ccdc-staging'
    )

    tpg_staging_tables = DummyOperator(
        task_id='external-tpg-staging'
    )

    amex_business_staging_tables = DummyOperator(
        task_id='external-amex-business-staging'
    )

    amex_consumer_staging_tables = DummyOperator(
        task_id='external-amex-consumer-staging'
    )

# Staging Dependencies
session_staging >> traffic_sources_staging
session_staging >> paidsearch_staging

# CCDC Staging Dependencies
[page_view_staging, page_metrics_staging, product_clicked_staging, product_viewed_staging, element_clicked_staging, element_viewed_staging, cookie_identified_staging,
    field_inputted_staging, device_staging, location_staging, decsion_staging, traffic_sources_staging, form_submitted_staging,
    paidsearch_staging] >> ccdc_staging_tables

# TPG Staging Dependencies
[page_view_staging, page_metrics_staging, product_clicked_staging, product_viewed_staging, element_clicked_staging, element_viewed_staging, cookie_identified_staging,
    field_inputted_staging, device_staging, location_staging, decsion_staging, traffic_sources_staging, form_submitted_staging, amp_page_viewed_staging,
    paidsearch_staging] >> tpg_staging_tables

# Amex Business Dependencies
[page_view_staging, page_metrics_staging, product_clicked_staging, product_viewed_staging, element_clicked_staging, element_viewed_staging,
    device_staging, location_staging, decsion_staging, traffic_sources_staging, form_submitted_staging,
    paidsearch_staging] >> amex_business_staging_tables

# Amex Consumer Dependencies
[page_view_staging, page_metrics_staging, product_clicked_staging, product_viewed_staging, element_clicked_staging,
    element_viewed_staging, device_staging, location_staging, decsion_staging, traffic_sources_staging,
    paidsearch_staging] >> amex_consumer_staging_tables
