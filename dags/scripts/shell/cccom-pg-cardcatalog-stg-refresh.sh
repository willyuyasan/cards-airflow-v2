# this is script which refreshes payman schema in postgresql from prod . This job MUST be switched off in
# Dev and Qa env.
#
echo "Start  time: $(date)"

FILEPATH=${DUMP_FILEPATH}
FILEPATH+="/"
SCHEMATODUMP=cardcatalog
DBUSER=${PGSQL_USER}

if [ {{params.refresh_env}} == 'dev' ] || [ {{params.refresh_env}} == 'qa' ]
then
  exit 0
fi


SQL="select array_to_string(array_agg('-T '||schemaname||'.'||tablename order by tablename ASC), ' ') from pg_tables where schemaname = '${SCHEMATODUMP}'"
SQL="${SQL} and ( tablename in ( 'flyway_schema_history', 'schema_version' ) or tablename like '%old%' or tablename like '%2019%' or"
SQL="${SQL} tablename like '%2020%' or tablename like '%2021%' or tablename like '%2bd%' ) "
SQL="${SQL} group by schemaname;"
echo "First Connection Start"
EXTBLIST=(`PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost}} --port 5432 -d {{params.db}} -t -c "${SQL}"`)
echo "First Connection End"
echo "EXTBLIST : "
echo $EXTBLIST
# prep the list of table to be refreshed and that list will be used for Truncate , drop FK and add FK steps
SQL="select array_to_string(array_agg(''''||tablename||'''' order by tablename ASC), ' ') from pg_tables where schemaname = '${SCHEMATODUMP}'"
SQL="${SQL} and ( tablename not in ( 'flyway_schema_history', 'schema_version' ) and tablename not like '%old%' and tablename not like '%2019%' and"
SQL="${SQL} tablename not like '%2020%' and tablename not like '%2021%' and tablename not like '%2bd%' )"
SQL="${SQL} group by schemaname;"
echo "Second Connection Start"
INCLUDETBLIST=(`PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost}} --port 5432 -d {{params.db}} -t -c "${SQL}"`)
echo "Second Connection End"
echo "INCLUDETBLIST : "
echo $INCLUDETBLIST
# prep the copy to csv list of table to be refreshed- creating csv file with data
SQL="select '\copy '||schemaname||'.'||tablename||' to '''||'${FILEPATH}'||tablename||'.csv'' with delimiter '','' csv header;' "
SQL="${SQL} from pg_tables where schemaname = '${SCHEMATODUMP}' and tablename in ( ${INCLUDETBLIST} ) Order by tablename;"
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost}} --port 5432 -d {{params.db}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_copy_tb_data_to_csv_file.sql

# prep the copy to csv list of table to be refreshed- loading the tables from csv file
SQL="select '\copy '||schemaname||'.'||tablename||' from '''||'${FILEPATH}'||tablename||'.csv'' with delimiter '','' csv header;' "
SQL="${SQL} from pg_tables where schemaname = '${SCHEMATODUMP}' and tablename in ( ${INCLUDETBLIST} ) Order by tablename;"
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost}} --port 5432 -d {{params.db}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_copy_tb_data_from_csv_file.sql

# actual dumping
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost}} --port 5432 -d {{params.db}} -q -f ${FILEPATH}${SCHEMATODUMP}_copy_tb_data_to_csv_file.sql


# prep the FK list for Drop
SQL="SELECT 'ALTER TABLE '||nsp.nspname||'.'||rpad(rel.relname,30,' ')||'DROP CONSTRAINT '||con.conname||';' FROM pg_catalog.pg_constraint con INNER JOIN pg_catalog.pg_class rel "
SQL="${SQL} ON rel.oid = con.conrelid and rel.relname in ( ${INCLUDETBLIST} ) INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace WHERE nsp.nspname = '${SCHEMATODUMP}' and con.contype = 'f';"

PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_drop_fk.sql


# prep the FK list for add

SQL="select distinct 'ALTER TABLE '||tco.table_schema||'.'||tco.table_name||'    ADD CONSTRAINT '||"
SQL="${SQL} tco.constraint_name ||' FOREIGN KEY ('||kcu.column_name||') REFERENCES '|| rel_kcu.table_schema ||"
SQL="${SQL} '.'||rel_kcu.table_name||' ('||rel_kcu.column_name||') MATCH SIMPLE ON UPDATE '||rco.update_rule|| "
SQL="${SQL} ' ON DELETE '|| rco.delete_rule || ';' from information_schema.table_constraints tco join "
SQL="${SQL} information_schema.key_column_usage kcu on tco.constraint_schema = kcu.constraint_schema and "
SQL="${SQL} tco.constraint_name = kcu.constraint_name join information_schema.referential_constraints rco  "
SQL="${SQL} on tco.constraint_schema = rco.constraint_schema and tco.constraint_name = rco.constraint_name "
SQL="${SQL} join information_schema.key_column_usage rel_kcu  on rco.unique_constraint_schema = rel_kcu.constraint_schema "
SQL="${SQL} and rco.unique_constraint_name = rel_kcu.constraint_name and kcu.ordinal_position = rel_kcu.ordinal_position "
SQL="${SQL} where tco.constraint_type = 'FOREIGN KEY' and tco.constraint_schema = '${SCHEMATODUMP}' and tco.table_name in ( ${INCLUDETBLIST} );"

PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_add_fk.sql

# prep the Truncate list

SQL="select 'TRUNCATE table '||table_schema||'.'||table_name||' ;' from information_schema.tables where table_schema = '${SCHEMATODUMP}'"
SQL="${SQL} and table_type = 'BASE TABLE' and table_name in ( ${INCLUDETBLIST} );"

PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_truncate.sql


# actual refresh steps

PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_drop_fk.sql
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_truncate.sql
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_copy_tb_data_from_csv_file.sql
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_add_fk.sql


#fix the sequence issue - going out of sync
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "truncate table public.cardcatalog_seq_verify;"

SQL="select 'insert into public.cardcatalog_seq_verify ( schema_name, tab_name, seq_name, b_max_id_val, b_last_value_seq)"
SQL="${SQL} values (''' || aa.table_schema||''' , '''|| aa.table_name || ''' , '''|| aa.seq_name||''' ,( select max(' || "
SQL="${SQL} aa.column_name ||') from ' || aa.table_schema || '.' || aa.table_name ||') , ( select last_value from ' || aa.seq_name || '));'"
SQL="${SQL} from ( select bb.* from ( select table_schema table_schema, table_name	table_name, column_name	column_name, "
SQL="${SQL} replace((replace(column_default,'nextval(''','')),'''::regclass)','') seq_name from information_schema.columns "
SQL="${SQL} where table_schema = 'cardcatalog' and table_name   not in ('flyway_schema_history') "
SQL="${SQL} order by table_schema, table_name, column_name) bb where bb.seq_name like 'cardcatalog.%' ) aa;"


PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_sequence_verify_get.sql
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_sequence_verify_get.sql

SQL="select 'SELECT setval(''' || seq_name || ''', ' || b_max_id_val || ', true);' from public.cardcatalog_seq_verify"
SQL="${SQL} where b_max_id_val is not null and ( b_max_id_val <> b_last_value_seq and b_last_value_seq < b_max_id_val );"

PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_sequence_set_exec.sql
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_sequence_set_exec.sql

SQL="select 'update public.cardcatalog_seq_verify set a_max_id_val = ( select max(' || aa.column_name || ') from ' ||"
SQL="${SQL} aa.table_schema || '.' || aa.table_name || '), a_last_value_seq = ( select last_value from ' || aa.seq_name ||"
SQL="${SQL} '), updated_date = now() where tab_name = ''' || aa.table_name || ''';' from ( "
SQL="${SQL} select table_schema table_schema, table_name	table_name, column_name column_name,"
SQL="${SQL} replace((replace(column_default,'nextval(''','')),'''::regclass)','') seq_name from information_schema.columns"
SQL="${SQL} where table_schema     = 'cardcatalog' and table_name not in ('flyway_schema_history') and ordinal_position = 1"
SQL="${SQL} order by  table_schema, table_name, column_name ) aa; "

PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_sequence_verify_upd.sql
PGPASSWORD="${CARDCATPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_sequence_verify_upd.sql

# house keeping
tar -czf ${FILEPATH}${SCHEMATODUMP}_dump.tar.gz ${FILEPATH}*.csv
rm ${FILEPATH}*.csv
tar -czf ${FILEPATH}${SCHEMATODUMP}_support.tar.gz ${FILEPATH}*.sql
rm ${FILEPATH}*.sql

# write the files in S3 bucket for QA refresh . QA refresh job runs only on Sunday / weekly which takes these files to refresh the QA env
aws s3 rm s3://cccom-dwh-qa/import/pg-db-refresh/ --recursive --exclude "*.*" --include "cardcatalog*.*" --profile cccom-des-dev-qa
aws s3 cp /home/airflow/temp_working/cardcatalog_refresh/ s3://cccom-dwh-qa/import/pg-db-refresh/ --recursive --exclude "*.*" --include "cardcatalog*.*" --profile cccom-des-dev-qa

echo "End  time: $(date)"
