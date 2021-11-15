# this is script which refreshes the 3 dimension tables in Affiliated_reporting
# portal schema ( in postgreSQL)

# . /home/airflow/.arp_cred
# connect to Mysql to get a dump of the data

SQL="select concat('select affiliate_reporting.proc_dim_affiliates_refresh(''', a.affiliate_id , ''',''',replace(a.company_name,'''',''''''),''',''',a.email,''',''',replace(a.first_name,'''',''''''),''',''',replace(a.last_name,'''',''''''),''',''',a.status,''',',(case when a.in_house then '''True''' else '''False''' end ),',''',a.time_modified,''',''', a.time_inserted,''');') from cccomus.partner_affiliates a where a.deleted = 0 order by a.time_inserted desc;"
mysql -u ${MYSQL_DB_USER} -p${MYSQL_DB_PASS} -h ${MYSQL_DBHOST} -AN -e"${SQL}" > ${DUMP_FILEPATH}/arp_dim_refresh.sql

# connect to Postgresql to load the data dumped in the previous step
PGPASSWORD="${DIMPASSWORD}" psql -h ${PGSQL_DIM_HOST} -d ${PGSQL_DIM_DB} -U ${PGSQL_DIM_USER} -f ${DUMP_FILEPATH}/arp_dim_refresh.sql -o ${DUMP_FILEPATH}/arp_dim_affiliates_refresh_out.log

ls -lhrt ${DUMP_FILEPATH}/arp_dim_refresh.sql
rm ${DUMP_FILEPATH}/arp_dim_refresh.sql

SQL="select concat('select affiliate_reporting.proc_dim_websites_refresh(',a.website_id,',''',a.affiliate_id,''',''', replace(a.url,'''','''''') /*replace(replace(replace(a.url,'''',''''''),'=\\','=\\\\'),'/?','//?')*/, ''',''', a.status, ''',''',now(),''',''', now(),''');') from cccomus.partner_websites a where a.deleted = 0;"
mysql -u ${MYSQL_DB_USER} -p${MYSQL_DB_PASS} -h ${MYSQL_DBHOST} -AN -e"${SQL}" > ${DUMP_FILEPATH}/arp_dim_refresh.sql

# connect to Postgresql to load the data dumped in the previous step
PGPASSWORD="${DIMPASSWORD}" psql -h ${PGSQL_DIM_HOST} -d ${PGSQL_DIM_DB} -U ${PGSQL_DIM_USER} -f ${DUMP_FILEPATH}/arp_dim_refresh.sql -o ${DUMP_FILEPATH}/arp_dim_websites_refresh_out.log

ls -lhrt ${DUMP_FILEPATH}/arp_dim_refresh.sql
rm ${DUMP_FILEPATH}/arp_dim_refresh.sql

SQL="select concat('select affiliate_reporting.proc_dim_categories_refresh(', a.page_id,',''',replace(a.page_name,'''',''''''),''',''', a.insert_time,''',''', now(),''');') from cccomus.pages a where a.deleted = 0;"
mysql -u ${MYSQL_DB_USER} -p${MYSQL_DB_PASS} -h ${MYSQL_DBHOST} -AN -e"${SQL}" > ${DUMP_FILEPATH}/arp_dim_refresh.sql

# connect to Postgresql to load the data dumped in the previous step
PGPASSWORD="${DIMPASSWORD}" psql -h ${PGSQL_DIM_HOST} -d ${PGSQL_DIM_DB} -U ${PGSQL_DIM_USER} -f ${DUMP_FILEPATH}/arp_dim_refresh.sql -o ${DUMP_FILEPATH}/arp_dim_categories_refresh_out.log

ls -lhrt ${DUMP_FILEPATH}/arp_dim_refresh.sql
rm ${DUMP_FILEPATH}/arp_dim_refresh.sql

# housekeeping
ls -lhrt ${DUMP_FILEPATH}/arp_dim_affiliates_refresh_out.log
ls -lhrt ${DUMP_FILEPATH}/arp_dim_websites_refresh_out.log
ls -lhrt ${DUMP_FILEPATH}/arp_dim_categories_refresh_out.log

rm ${DUMP_FILEPATH}/arp_dim_affiliates_refresh_out.log
rm ${DUMP_FILEPATH}/arp_dim_websites_refresh_out.log
rm ${DUMP_FILEPATH}/arp_dim_categories_refresh_out.log
