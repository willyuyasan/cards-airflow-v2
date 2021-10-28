# this should only run in PROD

# setting variables
FILEPATH=${DUMP_FILEPATH}
FILEPATH+="/"
FILENAME=vacuum_list_tables_{{params.file_suffix}}.sql
LOG_FILE=vacuum_list_tables_{{params.file_suffix}}.log


# display incoming variables passed
echo {{params.refresh}} {{params.pg_db_host}} {{params.pg_db_name}} {{params.pg_db_user}} {{params.file_suffix}}

# if it is in Dev , exit the program
if [ {{params.refresh}} == 'dev' ] || [ {{params.refresh}} == 'qa' ]
then
  exit 0
fi

#prep table list for vacuum
SQL="select 'vacuum (verbose, analyze) '||schemaname||'.'||tablename||';' from pg_tables where schemaname in ( 'transactions',"
SQL="${SQL} 'revenue_manager','pay_manager','parsing_service1','parsing_service','media_partner_program','loyalty_programs',"
SQL="${SQL}	'docman','docman1','cardmatch','cardcatalog','business_units','affiliate_reporting') and ( tablename not in "
SQL="${SQL} ( 'schema_version','flyway_schema_history') and ((tablename not like '%_1%') and (tablename not like '%old%')))"
SQL="${SQL} order by schemaname ,   tablename;"

#prep actual vacuum table list
PGPASSWORD="${STGPASSWORD}" psql -t -U {{params.pg_db_user}} -h {{params.pg_db_host}} -d {{params.pg_db_name}} -c "${SQL}" -o ${FILEPATH}${FILENAME}

# actual running vacuum
PGPASSWORD="${STGPASSWORD}" psql -h {{params.pg_db_host}} -d {{params.pg_db_name}} -U {{params.pg_db_user}} -f ${FILEPATH}${FILENAME} -o ${FILEPATH}${LOG_FILE}

