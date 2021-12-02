import base64
from pprint import pformat
from loguru import logger

from hooks.finserv_hook import FinServDatabricksHook

from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.utils.decorators import apply_defaults
import zlib

import json
import requests
from requests.auth import AuthBase
from threading import Thread
import time

"""
"""


# All the required fields are documented here -
# https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobssparksubmittask

class FinServDatabricksSubmitRunOperator(DatabricksSubmitRunOperator):
    """Execute a Spark job on Databricks."""

    @apply_defaults
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
                 cluster_permissions=None,
                 databricks_conn_id='databricks_default',
                 polling_period_seconds=30,
                 databricks_retry_limit=3,
                 databricks_retry_delay=1,
                 do_xcom_push=False,
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
        if cluster_permissions is not None:
            self.cluster_permissions = cluster_permissions

    @staticmethod
    def sidecar_thread_main(obj):
        obj.wait_for_cluster_id()
        obj.update_cluster_permissions()

    def start_sidecar(self):
        self.__stop_sidecar = False
        self.__sidecar = Thread(target=self.sidecar_thread_main, args=(self,))
        self.__sidecar.start()

    def stop_sidecar(self):
        self.__stop_sidecar = True
        self.__sidecar.join()

    def wait_for_cluster_id(self):
        while self.cluster_id is None and not self.__stop_sidecar:
            time.sleep(30)

    def deserialized_of(self, s):
        return json.loads(s) if isinstance(s, str) else s

    def update_cluster_permissions(self):
        if not self.__stop_sidecar:
            if not self.cluster_permissions:
                self.log.warning("Cluster permissions not set: no configuration found.")
            else:
                endpoint = f"api/2.0/permissions/clusters/{self.cluster_id}"
                hook = self.get_hook()
                host = hook.databricks_conn.host

                if 'token' in hook.databricks_conn.extra_dejson:
                    self.log.info('Using token auth.')
                    auth = _TokenAuth(hook.databricks_conn.extra_dejson['token'])
                else:
                    self.log.info('Using basic auth.')
                    auth = (hook.databricks_conn.login, hook.databricks_conn.password)

                url = f"https://{host}/{endpoint}"
                response = requests.patch(
                    url,
                    json=self.deserialized_of(self.cluster_permissions),
                    auth=auth,
                    timeout=hook.timeout_seconds,
                )
                if response.ok:
                    pass
                    self.log.info(
                        "Cluster permissions successfully set: %s",
                        json.dumps(response.json(), indent=2),
                    )
                else:
                    self.log.warning(
                        "Failed to set cluster permissions: %s, %s, %s",
                        str(response),
                        response.text,
                    )

    def get_hook(self):
        return FinServDatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay)

    def execute(self, context):
        self.log.debug("Running {} with parameters:\n{}".format(self.job_name, pformat(self.json)))
        # Attempt to execute the Databricks job
        try:
            self.start_sidecar()
            super(FinServDatabricksSubmitRunOperator, self).execute(context)
            self.stop_sidecar()
        except AirflowException as ex:
            # TODO: Write some more detail on why the task failed
            raise ex

    @property
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
        return self._cluster_id


class _TokenAuth(AuthBase):
    """
    Helper class for requests Auth field. AuthBase requires you to implement the __call__
    magic function.
    """

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r


class FinServDatabricksPlugin(AirflowPlugin):
    name = 'finserv_databricks'
    operators = [FinServDatabricksSubmitRunOperator]
