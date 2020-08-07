#!/bin/bash
# Author: Carmen De Vito <cdevito@redventures.com>
# Description: Install all the required components to get this up and running

# TODO: MAC Support
BREW_INSTALLED=$(which brew &>/dev/null; echo $?)
DOCKER_INSTALLED=$(which docker &>/dev/null; echo $?)
DOCKER_COMPOSE_INSTALLED=$(which docker-compose &>/dev/null; echo $?)
MAKE_INSTALLED=$(which make &>/dev/null; echo $?)
DOCKER_REGISTRY=redventures-cdm-docker.jfrog.io
CIRCLE_PROJECT_REPONAME="airflow-template"
HYPERKIT_CONF=${HOME}/Library/Containers/com.docker.docker/Data/vms/0/hyperkit.json

function bool_prompt() {
  APP=$1
  VALUE=$2

  if [[ ${VALUE} -eq 0 ]]; then
    echo "${APP}: true"
  else
    echo "${APP}: false"
  fi
}

echo "Developer Infromation:"
if [[ "${OSTYPE}" == "darwin"* ]]; then
  bool_prompt "Brew" ${BREW_INSTALLED}
fi
bool_prompt "Docker" ${DOCKER_INSTALLED}
bool_prompt "Docker-compose" ${DOCKER_COMPOSE_INSTALLED}
bool_prompt "Make" ${MAKE_INSTALLED}

if [[ $DOCKER_INSTALLED -ne 0 || $DOCKER_COMPOSE_INSTALLED -ne 0 ]]; then
  echo "Missing Docker"
  if [[ $BREW_INSTALLED -ne 0 && "$OSTYPE" == "darwin"* ]]; then
    echo "Missing Brew. Installing now"
    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
  fi

  # Try and install this on various systems
  echo "Installing Docker and Docker-compose"
  if [[ "$OSTYPE" == "darwin"* ]]; then
    brew cask install docker 
    brew install docker-compose
  elif [[ "$OSTYPE" == "" ]]; then
    dnf -y install docker docker-compose
  elif [[ "$OSTYPE" == "debian" ]]; then
    apt-get install docker docker-compose
  elif [[ "$OSTYPE" == "alpine" ]]; then
    apk add docker docker-compose
  fi
fi

# Check to see if docker is running
if ! docker info &> /dev/null ; then
  echo "Docker is installed, but it appears not to be running. Please make sure the Docker daemon is up and running"
  exit 1
fi

if ! grep "\"memory\":4096" ${HYPERKIT_CONF} &> /dev/null; then
  echo "WARN: It is recommended that you run your Docker Engine with at least 4GBs of memory."
fi

# Configure Artifactory Secrets if not defined locally. Used to pull Python packages and RV-contributed Docker images
if [[ ! -f .secrets ]]; then
  echo "It looks like you've never set up your credentials to start locally working with this setup. Let me help you with that"
  echo "First, we'll need to log into Artifactory and create a user token for this work. The token will be used to download Common"
  echo "libraries provided by the CDM team to make it easier to use Airflow."

  echo ""
  echo "First, let make sure you're logged into Artifactory via Okta"
  open "https://redventures.okta.com/home/redventures_artifactoryrvnew_1/0oa1gjnqmezVVvEyi0h8/aln1gjnzxjgqeTNlB0h8?fromHome=true"
  echo -n "Once you are logged in, hit enter to continue"
  read CHOICE

  echo "Next, we'll take you to your User profile."
  open "https://redventures.jfrog.io/redventures/webapp/#/profile"
  echo -n "What's your API key? It should be visible on your User Profile: " 
  read ARTIFACTORY_TOKEN -s

  echo -e "export ARTIFACTORY_USER=\"${USER}\"\nexport ARTIFACTORY_TOKEN=\"${ARTIFACTORY_TOKEN}\"\n" > .secrets
  echo "Ok, your Artifactory credentials have been updated. You shouldn't have to run this again, unless you recreate this repo or delete the .secrets file"
fi

# Once we've confirmed we have everything installed, let's look at setting up our container setup
echo "Baseline components are installed. Let's try and build this thing"
echo "Starting up development environment..."
source .secrets
docker login -u ${ARTIFACTORY_USER} -p ${ARTIFACTORY_TOKEN} redventures-cdm-docker.jfrog.io
docker-compose up
docker-compose down --rmi local
docker rmi local-airflow
