"""
# Staging Database Refresh

A script that will update all important data from Mantle to the AWS Staging environment.

"""

from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
from rvairflow import slack_hook as sh
from airflow.operators.python_operator import PythonOperator
import MySQLdb
from airflow.hooks.base_hook import BaseHook
import logging
import os
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 26),
    'email': ['rshukla@redventures.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=1),
    'queue': 'io_intensive_queue',
    'on_failure_callback': sh.slack_failure_callback(slack_connection_id=Variable.get("slack-connection-name"))
}

# Config Root
cfg_root = '/home/airflow/airflow/dags/core_staging'
source_user_connection = 'core-prod-ro-user'

airflow_core_stg_password = Variable.get('airflow_core_stg_password')
airflow_core_stg_user = Variable.get('airflow_core_stg_user')  # airflow
core_stg_endpoint = Variable.get('core_stg_endpoint')  # 'core-staging.ctcprtpk8atn.us-west-2.rds.amazonaws.com'

superdump_cmd = '''
        mysqlsuperdump -o {outfile} {config}
        exit_status=$?
        if [[ $exit_status -eq 0 ]]; then
            echo "`date`: mysqlsuperdump is completed successfully for {db}"
        else
            echo "`date`: mysqlsuperdump is failed for {db}"
        fi
'''

import_cmd = "mysql -u{user} -p{pwd} -h{host} {db} < {filename}"
cleanup_cmd = "rm {{path}}"


def export_schema_views_method(schema, source_connection, **kwargs):
    connection = BaseHook.get_connection(source_connection)
    db_user = connection.login
    db_password = connection.password
    db_host = connection.host
    db_name = connection.schema
    schema_name = schema

    cnx = MySQLdb.connect(user=db_user, passwd=db_password, host=db_host, db=db_name)
    cursor = cnx.cursor()
    cursor.execute(
        "select table_name from information_schema.tables where table_type = 'VIEW' and table_schema = '" + schema_name + "'")
    viewsString = ''
    for row in cursor:
        viewsString = viewsString + row[0] + ' '

    viewquery = "mysqldump -h '" + db_host + "' -u " + db_user + " -p" + db_password + " --lock-tables=false " + schema_name + \
        " " + viewsString + " | sed -e 's/DEFINER[ ]*=[ ]*[^*]*\\*/\\*/'  > " + cfg_root + "/out/" + schema_name + "_views.sql"
    logging.info(viewquery)
    os.system(viewquery)

    cursor.execute(
        "select distinct(event_object_table) from information_schema.triggers where trigger_schema = '" + schema_name + "'")
    viewsString = ''
    for row in cursor:
        procString = viewsString + row[0] + ' '

    procString = "mysqldump -h '" + db_host + "' -u " + db_user + " -p" + db_password + " --lock-tables=false --routines --no-create-info --no-data --no-create-db " + \
        schema_name + " | sed 's/DEFINER.*FUNCTION/FUNCTION/' | sed 's/DEFINER.*PROCEDURE/PROCEDURE/' >> " + cfg_root + "/out/" + schema_name + "_views.sql"
    logging.info(procString)
    os.system(procString)


with DAG('core_staging_data_refresh',
         default_args=default_args,
         schedule_interval="@daily",
         dagrun_timeout=timedelta(hours=8)) as dag:
    #    export_ccdata = BashOperator(
    #        task_id='export_ccdata',
    #        bash_command=superdump_cmd.format(outfile=cfg_root + '/out/ccdata.sql',
    #                                          config=cfg_root + '/config/ccdata-staging.cfg',
    #                                          db='ccdata'))

    export_nvmailer = BashOperator(
        task_id='export_nvmailer',
        bash_command=superdump_cmd.format(outfile=cfg_root + '/out/nvmailer.sql',
                                          config=cfg_root + '/config/nvmailer-staging.cfg',
                                          db='nvmailer'))

    export_cccomus = BashOperator(
        task_id='export_cccomus',
        bash_command=superdump_cmd.format(outfile=cfg_root + '/out/cccomus.sql',
                                          config=cfg_root + '/config/cccomus-staging.cfg',
                                          db='cccomus'))

    export_cms = BashOperator(
        task_id='export_cms',
        bash_command=superdump_cmd.format(outfile=cfg_root + '/out/cms.sql',
                                          config=cfg_root + '/config/cms-staging.cfg',
                                          db='cms'))

    export_cms_views = PythonOperator(
        task_id='export_cms_views',
        python_callable=export_schema_views_method,
        op_kwargs={'schema': 'cms',
                   'source_connection': source_user_connection},
        provide_context=True)

    export_cccomus_views = PythonOperator(
        task_id='export_cccomus_views',
        python_callable=export_schema_views_method,
        op_kwargs={'schema': 'cccomus',
                   'source_connection': source_user_connection},
        provide_context=True)

    export_nvmailer_views = PythonOperator(
        task_id='export_nvmailer_views',
        python_callable=export_schema_views_method,
        op_kwargs={'schema': 'nvmailer',
                   'source_connection': source_user_connection},
        provide_context=True)

    export_cardbank_views = PythonOperator(
        task_id='export_cardbank_views',
        python_callable=export_schema_views_method,
        op_kwargs={'schema': 'cardbank',
                   'source_connection': source_user_connection},
        provide_context=True)

    #    export_ccdata_views = PythonOperator(
    #        task_id='export_ccdata_views',
    #        python_callable=export_schema_views_method,
    #        op_kwargs={'schema': 'ccdata',
    #                   'source_connection': source_user_connection},
    #        provide_context=True)

    export_cardbank = BashOperator(
        task_id='export_cardbank',
        bash_command=superdump_cmd.format(outfile=cfg_root + '/out/cardbank.sql',
                                          config=cfg_root + '/config/cardbank-staging.cfg',
                                          db='cardbank'))

    postprocess_cms = BashOperator(
        task_id='postprocess_cms',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='cms',
                filename=cfg_root + '/sql/cms-postprocess.sql'))

    #    import_ccdata = BashOperator(
    #        task_id='import_ccdata',
    #        bash_command = import_cmd
    #            .format(user=airflow_core_stg_user,
    #                    pwd=airflow_core_stg_password,
    #                    host=core_stg_endpoint,
    #                    db='ccdata',
    #                    filename=cfg_root + '/out/ccdata.sql'))

    import_nvmailer = BashOperator(
        task_id='import_nvmailer',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='nvmailer',
                filename=cfg_root + '/out/nvmailer.sql'))

    import_cccomus = BashOperator(
        task_id='import_cccomus',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='cccomus',
                filename=cfg_root + '/out/cccomus.sql'))

    import_cms = BashOperator(
        task_id='import_cms',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='cms',
                filename=cfg_root + '/out/cms.sql'))

    import_cms_views = BashOperator(
        task_id='import_cms_views',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='cms',
                filename=cfg_root + '/out/cms_views.sql'))

    import_cardbank_views = BashOperator(
        task_id='import_cardbank_views',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='cardbank',
                filename=cfg_root + '/out/cardbank_views.sql'))

    import_nvmailer_views = BashOperator(
        task_id='import_nvmailer_views',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='nvmailer',
                filename=cfg_root + '/out/nvmailer_views.sql'))

    import_cccomus_views = BashOperator(
        task_id='import_cccomus_views',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='cccomus',
                filename=cfg_root + '/out/cccomus_views.sql'))

    postprocess_cccomus = BashOperator(
        task_id='postprocess_cccomus',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='cccomus',
                filename=cfg_root + '/sql/cccomus-postprocess.sql'))

    postprocess_nvmailer = BashOperator(
        task_id='postprocess_nvmailer',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='nvmailer',
                filename=cfg_root + '/sql/nvmailer-postprocess.sql'))

    #    import_ccdata_views = BashOperator(
    #        task_id='import_ccdata_views',
    #        bash_command=import_cmd
    #            .format(user=airflow_core_stg_user,
    #                    pwd=airflow_core_stg_password,
    #                    host=core_stg_endpoint,
    #                    db='ccdata',
    #                    filename=cfg_root + '/out/ccdata_views.sql'))

    import_cardbank = BashOperator(
        task_id='import_cardbank',
        bash_command=import_cmd
        .format(user=airflow_core_stg_user,
                pwd=airflow_core_stg_password,
                host=core_stg_endpoint,
                db='cardbank',
                filename=cfg_root + '/out/cardbank.sql'))

    post_cms_cleanup = BashOperator(
        task_id='post_cms_cleanup',
        bash_command="rm " + cfg_root + '/out/cms.sql ;' + " rm " + cfg_root + '/out/cms_views.sql'
    )
    post_cardbank_cleanup = BashOperator(
        task_id='post_cardbank_cleanup',
        bash_command="rm " + cfg_root + '/out/cardbank_views.sql ;' + " rm " + cfg_root + '/out/cardbank.sql'
    )
    post_cccomus_cleanup = BashOperator(
        task_id='post_cccomus_cleanup',
        bash_command="rm " + cfg_root + '/out/cccomus.sql ;' + " rm " + cfg_root + '/out/cccomus_views.sql'
    )
    post_nvmailer_cleanup = BashOperator(
        task_id='post_nvmailer_cleanup',
        bash_command="rm " + cfg_root + '/out/nvmailer_views.sql ;' + " rm " + cfg_root + '/out/nvmailer.sql'
    )
export_cms >> import_cms >> export_cms_views >> import_cms_views >> postprocess_cms >> post_cms_cleanup
export_cardbank >> import_cardbank >> export_cardbank_views >> import_cardbank_views >> post_cardbank_cleanup
export_cccomus >> import_cccomus >> export_cccomus_views >> import_cccomus_views >> postprocess_cccomus >> post_cccomus_cleanup
# export_ccdata >> import_ccdata >> export_ccdata_views >> import_ccdata_views
export_nvmailer >> import_nvmailer >> export_nvmailer_views >> import_nvmailer_views >> postprocess_nvmailer >> post_nvmailer_cleanup
