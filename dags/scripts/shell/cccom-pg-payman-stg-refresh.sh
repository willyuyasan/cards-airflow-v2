# this is script which refreshes payman schema in postgresql from prod . This job MUST be switched off in
# Dev and Qa env.
#
FILEPATH=${DUMP_FILEPATH}
FILEPATH+="/"
SCHEMATODUMP=pay_manager
DBUSER=${PGSQL_USER}
# set PGPASSWORD = ${PAYMANPASS}
if [ {{params.refresh}} == 'dev' ] || [ {{params.refresh}} == 'qa' ]
then
  exit 0
fi
SQL="select array_to_string(array_agg('-T '||schemaname||'.'||tablename order by tablename ASC), ' ') from pg_tables where schemaname = '${SCHEMATODUMP}'"
SQL="${SQL} and ( tablename in ( 'flyway_schema_history', 'schema_version' ) or tablename like '%old%' or tablename like '%2019%' or"
SQL="${SQL} tablename like '%2020%' or tablename like '%2021%' or tablename like '%2bd%' ) "
SQL="${SQL} group by schemaname;"
echo "====execute1 ==="
EXTBLIST=`sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost}} --port 5432 -d {{params.db}} -t -c "${SQL}"`
#  echo $PGPASSWORD
echo "end"
# prep the list of table to be refreshed and that list will be used for Truncate , drop FK and add FK steps
SQL="select array_to_string(array_agg(''''||tablename||'''' order by tablename ASC), ', ') from pg_tables where schemaname = '${SCHEMATODUMP}'"
SQL="${SQL} and ( tablename not in ( 'flyway_schema_history', 'schema_version' ) and tablename not like '%old%' and tablename not like '%2019%' and"
SQL="${SQL} tablename not like '%2020%' and tablename not like '%2021%' and tablename not like '%2bd%' )"
SQL="${SQL} group by schemaname;"
INCLUDETBLIST=`sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost}} --port 5432 -d {{params.db}} -t -c "${SQL}"`


rm -f ${FILEPATH}${SCHEMATODUMP}_backup_*.sql.gz

sudo PGPASSWORD="${PAYMANPASS}" pg_dump10 -Fc -w  --file ${FILEPATH}/${SCHEMATODUMP}_backup_$(date +"%m_%d_%Y").sql --host {{params.dbhost}} --port 5432 --username ${DBUSER} --verbose --format=p --inserts --data-only ${EXTBLIST} --schema ${SCHEMATODUMP} {{params.db}}


# prep the FK list for Drop
SQL="SELECT 'ALTER TABLE '||nsp.nspname||'.'||rpad(rel.relname,30,' ')||'DROP CONSTRAINT '||con.conname||';' FROM pg_catalog.pg_constraint con INNER JOIN pg_catalog.pg_class rel "
SQL="${SQL} ON rel.oid = con.conrelid and rel.relname in ( ${INCLUDETBLIST} ) INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace WHERE nsp.nspname = '${SCHEMATODUMP}' and con.contype = 'f';"

sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_drop_fk.sql


# prep the FK list for add

SQL="select distinct 'ALTER TABLE '||tco.table_schema||'.'||tco.table_name||'    ADD CONSTRAINT '||tco.constraint_name ||' FOREIGN KEY ('||kcu.column_name||') REFERENCES '|| rel_kcu.table_schema
                     ||'.'||rel_kcu.table_name||' ('||rel_kcu.column_name||') MATCH SIMPLE ON UPDATE '||rco.update_rule|| ' ON DELETE '|| rco.delete_rule || ';'
       from information_schema.table_constraints tco
            join information_schema.key_column_usage kcu on tco.constraint_schema       = kcu.constraint_schema
                                                        and tco.constraint_name         = kcu.constraint_name
            join information_schema.referential_constraints rco  on tco.constraint_schema       = rco.constraint_schema
                                                                and tco.constraint_name         = rco.constraint_name
            join information_schema.key_column_usage rel_kcu  on rco.unique_constraint_schema   = rel_kcu.constraint_schema
                                                             and rco.unique_constraint_name     = rel_kcu.constraint_name
                                                             and kcu.ordinal_position                   = rel_kcu.ordinal_position
      where tco.constraint_type = 'FOREIGN KEY'
        and tco.constraint_schema = '${SCHEMATODUMP}'
        and tco.table_name in ( ${INCLUDETBLIST} );"

sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_add_fk.sql

# prep the Truncate list

SQL="select 'TRUNCATE table '||table_schema||'.'||table_name||' ;' from information_schema.tables where table_schema = '${SCHEMATODUMP}'"
SQL="${SQL} and table_type = 'BASE TABLE' and table_name in ( ${INCLUDETBLIST} );"

sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -t -c "${SQL}" -o ${FILEPATH}${SCHEMATODUMP}_truncate.sql


# actual refresh steps

sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_drop_fk.sql
sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_truncate.sql
sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_backup_$(date +"%m_%d_%Y").sql
sudo PGPASSWORD="${PAYMANPASS}" psql -U ${DBUSER} --host {{params.dbhost_to}} --port 5432 -d {{params.db_to}} -q -f ${FILEPATH}${SCHEMATODUMP}_add_fk.sql



# house keeping
gzip ${FILEPATH}${SCHEMATODUMP}_backup_$(date +"%m_%d_%Y").sql
