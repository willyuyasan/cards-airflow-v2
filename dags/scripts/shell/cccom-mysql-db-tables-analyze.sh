# this should only run in PROD

. /home/airflow/.analyze_tab_cred

# display incoming variables passed
echo {{params.env}} {{params.mysql_db_host}} {{params.mysql_db_host_stag}} {{params.mysql_db_user}}

# do the main - dev ( if enabled in Dev), qa ( if enabled in QA) and prod
mysql -u {{params.mysql_db_user}} -p${MYSQL_DB_PASS} -h {{params.mysql_db_host}} -sN -e "select schema_name from information_schema.schemata where schema_name not in ( 'information_schema','innodb','mysql','percona','performance_schema','sys');" | while read database therest;
        do
           mysql -u {{params.mysql_db_user}} -p${MYSQL_DB_PASS} -h {{params.mysql_db_host}} -sN -D$database -e "SHOW TABLE STATUS WHERE Data_free > 100 AND Data_free/Data_length > 0.1" | while read tblname therest;
           do
             mysqlcheck -u {{params.mysql_db_user}} -p${MYSQL_DB_PASS} -h {{params.mysql_db_host}} --analyze $database $tblname
           done
        done


# if it is in Dev and QA , exit the program
if [ {{params.env}} == 'dev' ] || [ {{params.env}} == 'qa' ]
then
  exit 0
fi

# do the staging - works only in prod
mysql -u {{params.mysql_db_user}} -p${MYSQL_DB_PASS_STAG} -h {{params.mysql_db_host_stag}} -sN -e "select schema_name from information_schema.schemata where schema_name not in ( 'information_schema','innodb','mysql','percona','performance_schema','sys');" | while read database therest;
        do
           mysql -u {{params.mysql_db_user}} -p${MYSQL_DB_PASS_STAG} -h {{params.mysql_db_host_stag}} -sN -D$database -e "SHOW TABLE STATUS WHERE Data_free > 100 AND Data_free/Data_length > 0.1" | while read tblname therest;
           do
             mysqlcheck -u {{params.mysql_db_user}} -p${MYSQL_DB_PASS_STAG} -h {{params.mysql_db_host_stag}} --analyze $database $tblname
           done
        done

