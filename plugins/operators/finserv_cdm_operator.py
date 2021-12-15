from airflow.utils.decorators import apply_defaults
from rvairflow.dbx.dbx_operator import CdmDatabricksSubmitRunOperator
from rvairflow.dbx.task import NewCluster, JarTask, NotebookParams, NotebookTask, SparkEnvVars, ClusterCustomTags, DagDefaultArgs
from rvairflow.cdm.params import RunnerParams
from rvairflow.cdm import const as cdm_const
from datetime import datetime
import copy

"""
"""

# All the required fields are documented here -
# https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobssparksubmittask


class FinServDatabricksSubmitRunOperator(CdmDatabricksSubmitRunOperator):
    """Execute a Spark job on Databricks."""

    @apply_defaults
    def __init__(self,
                 job_name=None,
                 task=None,
                 task_id=None,
                 json=None,
                 spark_jar_task=None,
                 notebook_task=None,
                 spark_python_task=None,
                 spark_submit_task=None,
                 new_cluster=None,
                 existing_cluster_id=None,
                 libraries=None,
                 run_name=None,
                 timeout_seconds=None,
                 cluster_permissions=None,
                 databricks_conn_id='databricks_default',
                 polling_period_seconds=30,
                 databricks_retry_limit=3,
                 databricks_retry_delay=1,
                 do_xcom_push=False,
                 log_retry=5,
                 log_sleep=20,
                 **kwargs):

        """__init__
        Generate parameters for running a job through the Databricks run-submit
        api.
        See: https://docs.databricks.com/api/latest/jobs.html#runs-submit

        Arguments:
            :param job_name: {str} -- Name of the job
            :param task: {obj} -- Instance of CdmNotebookTask or CdmJarTask classes
            :param databricks_conn_id: {str} -- The name of the Airflow connection to use. By default
                and in the common case this will be ``databricks_default``. To use token based
                authentication, provide the key ``token`` in the extra field for the connection
                and create the key ``host`` and leave the ``host`` field empty.
            :param timeout_seconds: {int} -- The timeout for this run. By default a value of 0 is used
                which means to have no timeout. (default: {900})
            :param polling_period_seconds: {int} -- Controls the rate which we poll for the result of
                this run. By default the operator will poll every 30 seconds (default: {30})
            :param databricks_retry_limit: {int} -- Amount of times retry if the Databricks backend is
                unreachable. Its value must be greater than or equal to 3. (default: {3})
            :param databricks_retry_delay: {int} -- Number of seconds to wait between retries (it
                might be a floating point number) (default: {1})
            :param do_xcom_push: {bool} -- Whether we should push run_id and run_page_url to xcom. (default: {False})
            :param log_retry: {int} -- Number of times to pull logs from DBFS. (default: {5})
            :param log_sleep: {int} -- Seconds to wait between log read failures. {default: {20}}
        """

        # Update finserv operator cluster information to match cdm format
        new_cluster = copy.deepcopy(new_cluster)
        log_path = new_cluster.pop('cluster_log_conf')['dbfs']['destination']
        custom_tags = new_cluster.pop('custom_tags')
        env_name = log_path.split('/')[-3]
        del new_cluster['auto_termination_minutes']
        new_cluster.update(new_cluster.pop('aws_attributes'))

        # Define cluster for CDM
        ct = ClusterCustomTags(**custom_tags)
        env = SparkEnvVars(cdm_secret_scope='cards', api_secret_scope='cards')
        cluster = NewCluster(spark_env_obj=env, custom_tags_obj=ct, **new_cluster)
        runner_param_list = [param.lower() for param in cdm_const.RUNNER_PARAMETERS.keys()]

        # Define runner for CDM
        if spark_jar_task:
            spark_jar_params = {param.split('=')[0].lower(): param.split('=')[1] for param in spark_jar_task['parameters'] if param.split('=')[0].lower() in runner_param_list}
            runner_params = RunnerParams(environment=env_name,
                                         etl_time=datetime.now().isoformat(),
                                         custom_parameter__dbx_secrets_scope='cards',
                                         **spark_jar_params)

            # Finally, define task using runner and cluster definitions
            task = JarTask(cluster=cluster,
                           params=runner_params,
                           main_class=spark_jar_task['main_class_name'],
                           jar_libraries=libraries,
                           tables=spark_jar_params.get('tables'))
        else:
            spark_jar_params = {param.split('=')[0].lower(): param.split('=')[1] for param in spark_jar_task['parameters'] if param.split('=')[0].lower() in runner_param_list}
            runner_params = RunnerParams(environment=env_name,
                                         etl_time=datetime.now().isoformat(),
                                         custom_parameter__dbx_secrets_scope='cards',
                                         **spark_jar_params)

            # Finally, define task using runner and cluster definitions
            task = NotebookTask(cluster=cluster,
                                params=runner_params,
                                main_class=spark_jar_task['main_class_name'],
                                jar_libraries=libraries,
                                tables=spark_jar_params.get('tables'))

        # Pass parameters to CDM class
        super().__init__(
            job_name=task_id.replace('-', '_'),
            task=task,
            databricks_conn_id=databricks_conn_id,
            libraries=libraries,
            timeout_seconds=timeout_seconds,
            polling_period_seconds=polling_period_seconds,
            databricks_retry_limit=databricks_retry_limit,
            databricks_retry_delay=databricks_retry_delay,
            do_xcom_push=do_xcom_push,
            log_retry=log_retry,
            log_sleep=log_sleep,
            run_name=run_name,
            cluster_permissions=cluster_permissions,
            spark_python_task=spark_python_task,
            spark_submit_task=spark_submit_task,
            existing_cluster_id=existing_cluster_id,
            **kwargs
        )
