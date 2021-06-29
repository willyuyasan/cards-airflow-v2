import sys
import sqlalchemy
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from airflow.hooks.base_hook import BaseHook


def full_vacuum_tables(**kwargs):
    # create a connection to redshift
    if kwargs['conn_type'] != 'postgres':
        raise ValueError('Incorrect connection type')
    else:
        kwargs['conn_type'] = 'postgresql'
    connection_uri = '{conn_type}://{login}:{password}@{host}:{port}/{schema}'.format(**kwargs)
    engine = sqlalchemy.create_engine(connection_uri)
    # query
    select_query = """
                    with tab_info as (
                    SELECT schema table_schema, "table" table_name
                    FROM svv_table_info a
                    where ((schema = 'cccom_dw'
                            and table_name similar to ('(ctl|dim|fact)%%'))
                        or schema in ({0}))
                    and (unsorted > 5
                       or stats_off > 10)
                    order by unsorted desc
                    ),
                    pg_tab as (
                    select relname tablename
                    from pg_class
                    where relowner = (select usesysid
                                    from  pg_user
                                    where usename = '{1}')
                    )
                    select ti.table_schema, ti.table_name
                    from tab_info ti
                    join pg_tab pt
                    on (ti.table_name = pt.tablename)
    """.format(kwargs['db_schema'], kwargs['login'])

    select_df = pd.read_sql(select_query, engine)
    print(select_df)
    raw_connection = engine.raw_connection()
    raw_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = raw_connection.cursor()
    # loop over the dataframe
    for index, row in select_df.iterrows():
        flag = True
        while flag:
            svv_vacuum_progress = "select * from svv_vacuum_progress"
            svv_vacuum_progress_df = pd.read_sql(svv_vacuum_progress, engine)

            if (svv_vacuum_progress_df['status'].iloc[0] == 'Complete' or
                    svv_vacuum_progress_df['status'].iloc[0] == 'Failed' or
                    svv_vacuum_progress_df['status'].iloc[0] == 'Skipped (delete only)' or
                    svv_vacuum_progress_df['status'].iloc[0] == 'Skipped' or
                    svv_vacuum_progress_df['status'].iloc[0] == 'Skipped(sorted>=95%)'):
                print("No Vacuum command is running, please continue")
                flag = False
            else:
                print("Vacuum is running, waiting for some time to finish up Vacuum before we can continue.")
                time.sleep(300)
                break
            params = {'table_schema': row['table_schema'],
                      'table_name': row['table_name']}
            print(params)
            try:
                vacuum_query = "vacuum full {table_schema}.{table_name};".format(**params)
                print(vacuum_query)
                cursor.execute(vacuum_query)
                analyze_query = "analyze {table_schema}.{table_name};".format(**params)
                print(analyze_query)
                cursor.execute(analyze_query)
            except Exception as e:
                if "VACUUM is running" in e:
                    print("Exception: %s.\n" % str(e))
                    flag = True
                    pass
            print("----------")
    raw_connection.close()


if __name__ == '__main__':
    try:

        redshift_connection = BaseHook.get_connection(sys.argv[1])

        kwargs = {
            "conn_type": redshift_connection.conn_type,
            "host": redshift_connection.host,
            "schema": redshift_connection.schema,
            "login": redshift_connection.login,
            "password": redshift_connection.password,
            "port": redshift_connection.port,
            "db_schema": sys.argv[2],
        }

        full_vacuum_tables(**kwargs)

    except Exception as e:
        print("Exception: %s.\n" % str(e))
        ph.print_exception()
        sys.exit(1)
