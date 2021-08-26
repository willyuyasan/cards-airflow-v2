# this is script which refreshes the 3 dimension tables in Affiliated_reporting
# portal schema ( in postgreSQL)

# . /home/airflow/.arp_cred_stag


# connect to Mysql to get a dump of the data

SQL="select concat('select affiliate_reporting.proc_dim_affiliates_refresh(''', a.affiliate_id , ''',''',replace(a.company_name,'''',''''''),''',''',a.email,''',''',replace(a.first_name,'''',''''''),''',''',replace(a.last_name,'''',''''''),''',''',a.status,''',',(case when a.in_house then '''True''' else '''False''' end ),',''',a.time_modified,''',''', a.time_inserted,''');') from cccomus.partner_affiliates a where a.deleted = 0 order by a.time_inserted desc;"

mysql -u {{params.MYSQL_DB_USER}} -p {{params.MYSQL_DB_PASS}} -h {{params.MYSQL_DBHOST}}-AN -e"${SQL}" > {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql

# connect to Postgresql to load the data dumped in the previous step
export PGPASSWORD={{params.PGSQL_DB_PASS}}
psql -h {{params.PGSQL_DBHOST}} -d {{params.PGSQL_DB}} -U {{params.PGSQL_DB_USER}} -f {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql -o {{params.DUMP_FILEPATH}}/arp_dim_affiliates_refresh_out_stag.log

rm {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql

SQL="select concat('select affiliate_reporting.proc_dim_websites_refresh(',a.website_id,',''',a.affiliate_id,''',''', replace(a.url,'''','''''') /*replace(replace(replace(a.url,'''',''''''),'=\\','=\\\\'),'/?','//?')*/, ''',''', a.status, ''',''',now(),''',''', now(),''');') from cccomus.partner_websites a where a.deleted = 0;"

mysql -u {{params.MYSQL_DB_USER}} -p {{params.MYSQL_DB_PASS}} -h {{params.MYSQL_DBHOST}}-AN -e"${SQL}" > {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql

# connect to Postgresql to load the data dumped in the previous step
export PGPASSWORD={{params.PGSQL_DB_PASS}}
psql -h {{params.PGSQL_DBHOST}} -d {{params.PGSQL_DB}} -U {{params.PGSQL_DB_USER}} -f {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql -o {{params.DUMP_FILEPATH}}/arp_dim_websites_refresh_out_stag.log

rm {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql


SQL="select concat('select affiliate_reporting.proc_dim_categories_refresh(', a.page_id,',''',replace(a.page_name,'''',''''''),''',''', a.insert_time,''',''', now(),''');') from cccomus.pages a where a.deleted = 0;"

mysql -u {{params.MYSQL_DB_USER}} -p {{params.MYSQL_DB_PASS}} -h {{params.MYSQL_DBHOST}}-AN -e"${SQL}" > {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql

# connect to Postgresql to load the data dumped in the previous step
export PGPASSWORD={{params.PGSQL_DB_PASS}}
psql -h {{params.PGSQL_DBHOST}} -d {{params.PGSQL_DB}} -U {{params.PGSQL_DB_USER}} -f {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql -o {{params.DUMP_FILEPATH}}/arp_dim_categories_refresh_out_stag.log

rm {{params.DUMP_FILEPATH}}/arp_dim_refresh_stag.sql

# housekeeping
rm {{params.DUMP_FILEPATH}}/arp_dim_affiliates_refresh_out_stag.log
rm {{params.DUMP_FILEPATH}}/arp_dim_websites_refresh_out_stag.log
rm {{params.DUMP_FILEPATH}}/arp_dim_categories_refresh_out_stag.log