import base64
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.databricks_hook import DatabricksHook
import zlib

LIST_DBFS_ENDPOINT = ('GET', 'api/2.0/dbfs/list')
READ_DBFS_ENDPOINT = ('GET', 'api/2.0/dbfs/read')


class FinServDatabricksHook(DatabricksHook):
    """
    Interact with Databricks.
    """

    def list_dbfs(self, path):
        """list_dbfs:
        Retrieves a list of objects in dbfs from a specified path
        """

        # Variables
        params = {
            'path': path
        }

        try:
            return self._do_api_call(LIST_DBFS_ENDPOINT, params)
        except AirflowException as ex:
            raise AirflowException(f"Failed to list {path}: {ex}")

    def read_dbfs(self, path):
        """get_dbfs_file
        Pull a file from dbfs. This can be used to retrieve cluster logs if the cluster_log_conf
        was defined.
        """

        # Variables
        params = {
            'path': path
        }

        fileContent = None

        try:
            response = self._do_api_call(READ_DBFS_ENDPOINT, params)
            if 'data' in response:
                if path.endswith('.gz'):
                    fileContent = zlib.decompress(base64.b64decode(response['data']), zlib.MAX_WBITS | 32)
                # data is base64-encoded bytes that needs to be decoded
                return base64.b64decode(response['data']) if fileContent is None else fileContent
        except AirflowException as ex:
            raise AirflowException(f"Failed to get {path}: {ex}")
