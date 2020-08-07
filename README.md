# cards-airflow

## Starting the local environment

To get started working with Airflow, do the following:

1. Clone this repo: `git clone -b develop git@github.com:RedVentures/cards-airflow.git`
2. Go into this directory and run `bin/setup.sh`: `cd cards-airflow && bin/setup.sh`

This script will set up your workstation to be run docker containers and spin up several components to create a basic
Airflow installation.

### Accessing Airflow

* Localhost
  * [Airflow UI](http://localhost:8080)
  * [Airflow Flower](http://localhost:5555)
* Development
  * [Airflow UI](https://app.airflow-development.cards-nonprod.rvapps.io)
  * [Airflow Flower](https://flower-development.airflow.cards-nonprod.rvapps.io)
* Production
  * [Airflow UI](https://app.airflow.cards-nonprod.rvapps.io)
  * [Airflow Flower](https://flower.airflow.cards-nonprod.rvapps.io)

## DAG Repo Components

Each DAG Repository created with the airflow-template will have the following:

* .circleci/ - CircleCI Workflow to test, build and publish custom Airflow Docker images for your infrastructure
* bin/setup.sh - bootstrap script for Data Engineers to start up a local development environment
* dags/ - Location where all your DAGs will be stored.
* docs/ - Common location for documentation related to your DAGs and Airflow installation.
* plugins/ - Flask plugins installed in Airflow
* tests/ - pyTests to validate DAG functionality (Optional)
* Dockerfile - docker image for local development environment. Can also be used with ECR to make custom images for AWS deployments.
* Makefile
* buildspec.yml - CodeBuild workflow for syncronizing DAGs with your AWS infrastructure.
* docker-compose.yml - Local development environment configuration
* requirements.txt - List of Python packages to bundle into the final Docker container.
* test-requirements.txt - List of Python packages to install as part of the testing phase of any CircleCI job.

In addition, we currently include the following examples:
* dags/example-dag.py - Spins up a DAG with a few `sleep $RANDOM && date` bash tasks. Shows declaration & dependency 
  syntax
* plugins/astronomer-docs.py - Documentation to the Astronomer project. Not currently used, but still useful.
* plugins/rv-docs.py - Adds links to this repo, your CircleCI job and the upstream puckel Docker image. 

## Branch Tracking

| Repo Branch | AWS Environment                         |
| ----------- | --------------------------------------- |
| `develop`   | nonprod (development, staging, testing) |
| `master`    | production                              |

## Troubleshooting

### Getting Stack Traces on Database Connections

This can happen if the postgres database was partially initialized, but never completed. The postgres and s3 buckets
have data that is persistent in the .data/ directory. In this case, we don't want to keep this data and it should be 
reinitialized.

1. `docker-compose down`
2. `rm -rf .data`
3. `bin/setup.sh`

Your cluster should come up cleanly within a few minutes.

## See Also

* [Airflow User Guide](https://cdm.rvdocs.io/docs/user-guide/user_guide.html)
* [Airflow Admin Guide](https://cdm.rvdocs.io/docs/user-guide/admin_guide.html)
* [RedVenturen/airflow-template](https://github.com/RedVentures/airflow-template)
* [Example DAG repo](https://github.com/RedVentures/airflow-dag-test/tree/develop)
* [Airflow Homepage](https://airflow.apache.org/)
* [Apache Airflow Documentation](https://airflow.apache.org/docs/stable/)
