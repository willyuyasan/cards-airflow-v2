import sys
import logging
import tempfile
import csv
from operators.unicode_csv_helper import UnicodeWriter
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.decorators import apply_defaults


class MySqlReplSafeUpdateOperator(MySqlOperator):
    """
    Executes a select statement to retrieve data to load into
    a table, and then performs a bulk load of that data.
    Creates a temporary file to do so.

    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrites the defined one in connection
    :type database: string
    :param table: the table to load data into
    :type sql: string
    :param duplicate_handling: Can be REPLACE, IGNORE, or nothing.  Affects bulk
        data load behavior, see MySQL documentation
    :type duplicate_handling: string
    """

    @apply_defaults
    def __init__(
            self, table=None, duplicate_handling=None,
            *args, **kwargs):
        super(MySqlReplSafeUpdateOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.duplicate_handling = duplicate_handling

    def execute(self, context):
        # Added by RZ to check SQL assignment. START
        # print("My SQL Safe Replace Version : ", sys.version)
        sql = str(self.sql)
        # Added by RZ to check SQL assignment. END
        logging.info('Retrieving entries:\n' + str(self.sql))
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        # The only difference between this and MySqlHook.get_pandas_df()
        # is that we're leaving the connection open after getting the  dataframe.
        if sys.version_info[0] < 3:
            sql = self.sql.encode('utf-8')
        conn = hook.get_conn()
        tmp = tempfile.NamedTemporaryFile(dir="/scratch")
        logging.info('LOG This is the Temporary File Name :: ' + tmp.name)
        print('This is the Temporary File Name :: ' + tmp.name)
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()

        with open(tmp.name, mode='wb') as csvfile:
            csv_writer = UnicodeWriter(csvfile, delimiter='\t')
            for row in result:
                csv_writer.writerow(row)

        logging.info('Loading data into {table}'.format(table=self.table))
        cur.execute("""
        LOAD DATA LOCAL INFILE '{tmp_file}'
        {mode} INTO TABLE {table}
        FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        """.format(tmp_file=tmp.name,
                   table=self.table,
                   mode=self.duplicate_handling))
        conn.commit()
        conn.close()
        tmp.close()
