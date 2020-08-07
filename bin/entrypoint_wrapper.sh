#!/usr/bin/env bash

# Before we start anything up really, let's make sure that initdb was executed first.
if [[ "$1" == "airflow" && "$2" == "webserver" ]]; then
  url_parse_regex="[^:]+://([^@/]*@)?([^/:]*):?([0-9]*)/?"

  # Wait for postgres then init the db
  if [[ -n $AIRFLOW__CORE__SQL_ALCHEMY_CONN  ]]; then
    # Wait for database port to open up
    [[ ${AIRFLOW__CORE__SQL_ALCHEMY_CONN} =~ $url_parse_regex ]]
    HOST=${BASH_REMATCH[2]}
    PORT=${BASH_REMATCH[3]}
    echo "Waiting for host: ${HOST} ${PORT}"
    while ! nc -w 1 -z "${HOST}" "${PORT}"; do
      sleep 0.001
    done
  fi

  airflow initdb
fi


/entrypoint.sh $@
