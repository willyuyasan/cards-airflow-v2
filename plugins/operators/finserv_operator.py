import base64
from pprint import pformat
from loguru import logger

from hooks.finserv_hook import FinServDatabricksHook

from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
import zlib

"""
"""


# All the required fields are documented here -
# https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobssparksubmittask

class FinServDatabricksSubmitRunOperator(DatabricksSubmitRunOperator):
    """Execute a Spark job on Databricks."""

    def __init__(self,
                 *,
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
                 databricks_conn_id='databricks_default',
                 polling_period_seconds=30,
                 databricks_retry_limit=3,
                 databricks_retry_delay=1,
                 do_xcom_push=False,
                 **kwargs,):
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
        """
        super(FinServDatabricksSubmitRunOperator, self).__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        if spark_jar_task is not None:
            self.json['spark_jar_task'] = spark_jar_task
        if notebook_task is not None:
            self.json['notebook_task'] = notebook_task
        if spark_python_task is not None:
            self.json['spark_python_task'] = spark_python_task
        if spark_submit_task is not None:
            self.json['spark_submit_task'] = spark_submit_task
        if new_cluster is not None:
            self.json['new_cluster'] = new_cluster
        if existing_cluster_id is not None:
            self.json['existing_cluster_id'] = existing_cluster_id
        if libraries is not None:
            self.json['libraries'] = libraries
        if run_name is not None:
            self.json['run_name'] = run_name
        if timeout_seconds is not None:
            self.json['timeout_seconds'] = timeout_seconds
        if 'run_name' not in self.json:
            self.json['run_name'] = run_name or kwargs['task_id']
        self._cluster_id = None
        self.job_name = run_name or kwargs['task_id']

    def get_hook(self):
        return FinServDatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay)

    def execute(self, context):
        self.log.debug("Running {} with parameters:\n{}".format(self.job_name, pformat(self.json)))

        # Attempt to execute the Databricks job
        try:
            super(FinServDatabricksSubmitRunOperator, self).execute(context)

            # Try and gather details about logging options in this cluster
            self._log_paths(context)
            self._print_logs()
        except AirflowException as ex:
            # TODO: Write some more detail on why the task failed
            # Still try and pull the logs if the run fails
            self._log_paths(context)
            self._print_logs()
            raise ex

    def _print_logs(self):
        """
        Try and output the related Spark and DBX Cluster logs if available.
        """
        if self.log_type is not None:
            hook = self.get_hook()
            try:
                # Retrieve Spark Logs
                self.log.info(f"Spark logs will be stored in {self.spark_logs}")
                spark_filelist = hook.list_dbfs(self.spark_logs)
                if spark_filelist is not None:
                    for f in spark_filelist['files']:
                        if not f['is_dir']:
                            self.log.info(f['path'])

                            if f['path'].endswith('.gz') and 'stderr' in f['path']:
                                self.log.info("The gzipped file is present on this location, please read the file from that location %s", f['path'])
                            elif 'stderr' in f['path']:
                                for line in hook.read_dbfs(f['path']).decode('utf-8').replace('\n', "\n").split("\n"):
                                    self.log.error(line)
                            else:
                                for line in hook.read_dbfs(f['path']).decode('utf-8').replace('\n', "\n").split("\n"):
                                    self.log.info(line)

                # Retrieve Executor Logs
                self.log.info(f"Executor logs will be stored in {self.exec_logs}")
                exec_filelist = hook.list_dbfs(self.exec_logs)
                if exec_filelist is not None:
                    for app in exec_filelist['files']:
                        for executor in hook.list_dbfs(app['path'])['files']:
                            for f in hook.list_dbfs(executor['path'])['files']:
                                if not f['is_dir']:
                                    self.log.info(f['path'])

                                    if f['path'].endswith('.gz') and 'stderr' in f['path']:
                                        self.log.info("The gzipped file is present on this location, please read the file from that location %s", f['path'])
                                    elif 'stderr' in f['path']:
                                        for line in hook.read_dbfs(f['path']).decode('utf-8').replace('\n', "\n").split("\n"):
                                            self.log.error(line)
                                    else:
                                        for line in hook.read_dbfs(f['path']).decode('utf-8').replace('\n', "\n").split("\n"):
                                            self.log.info(line)
            except zlib.error as e:
                self.log.info(e)

    def _log_paths(self, context):
        """
        Read in the cluster_log_conf parameter from the cluster running this job and extract the possible location of
        spark and executor logs. These will be stored as parameters spark_logs and exec_logs within this class
        :param context: Task context
        """
        self.log_type = None
        self.log_dest = None

        if 'cluster_log_conf' in self.json['new_cluster'] and len(self.json['new_cluster']['cluster_log_conf'].keys()):
            self.log_type = list(self.json['new_cluster']['cluster_log_conf'].keys())[0]
            self.log_dest = self.json['new_cluster']['cluster_log_conf'][self.log_type]['destination']
            if self._cluster_id is not None:
                self.spark_logs = '{}/{}/driver'.format(self.log_dest, self._cluster_id)
                self.exec_logs = '{}/{}/executor'.format(self.log_dest, self._cluster_id)
            else:
                self.cluster_id()
                self.spark_logs = '{}/{}/driver'.format(self.log_dest, self._cluster_id)
                self.exec_logs = '{}/{}/executor'.format(self.log_dest, self._cluster_id)

    def cluster_id(self):
        """
        Return the cluster_id this run is executing from. Requires an API call back to runs/get to pull the
        cluster_id
        """
        # Possible alternative "job-" + self.job_id + "-run-1"
        if self._cluster_id is None and self.run_id is not None:
            json = {'run_id': self.run_id}
            response = self.get_hook()._do_api_call(('GET', 'api/2.0/jobs/runs/get'), json)
            if 'cluster_instance' in response:
                self._cluster_id = response['cluster_instance']['cluster_id']


class FinServDatabricksPlugin(AirflowPlugin):
    name = 'finserv_databricks'
    operators = [FinServDatabricksSubmitRunOperator]
