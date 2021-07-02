#!/bin/bash
# Author: Carmen De Vito <cdevito@redventures.com>
# Description: Update the local dag repo

# NOTE: This script can be run either through GitHub actions
# or manually via a user's CLI.

# TODO: Check for inputted variables (non-interactive)
# NOTE: We will prompt if we are in an interactive shell for this
# GITHUB_REPO_NAME
# GITHUB_TOKEN
# COOKIECUTTER_CONF
# PR_TITLE
# PR_BODY
if [ -z "${GITHUB_REPO_NAME}" ]; then
  read -p "GITHUB_REPO_NAME: " GITHUB_REPO_NAME
fi
if [ -z "${GITHUB_TOKEN}" ]; then
  read -s -p "GITHUB_TOKEN: " GITHUB_TOKEN
fi
COOKIECUTTER_CONF=${COOKIECUTTER:=.cookiecutter.conf.yaml}
if [ -z "${PR_TITLE}" ]; then
  read -p "PR_TITLE: " PR_TITLE
fi
if [ -z "${PR_BODY}" ]; then
  read -p "PR_BODY: " PR_BODY
fi

# Check to see if cookiecutter has been installed
PY_PKGS=$(pip list 2>/dev/null)
if [[ $(echo "${PY_PKGS}" | grep cookiecutter | wc -l) -lt 1 ]]; then
  echo "Missing required cookiecutter tool. Install with pip install cookiecutter"
  exit 1
fi

# Check to see if jinja2 has been installed
if [[ $(echo "${PY_PKGS}" | grep jinja | wc -l) -lt 1 ]]; then
  echo "Missing required jinja2 tool. Install with pip install jinja2"
  exit 1
fi

# Check to see if the airflow-template repo has been pulled
if [[ ! -d RedVentures/repo ]]; then
  echo "Missing airflow-template repo."
  echo "git clone github.com/RedVentures/airflow-template repo"
  exit 1
fi

# Check to see if the local repo has been pulled
if [[ ! -d $GITHUB_REPO_NAME ]]; then
  echo "Missing ${GITHUB_REPO_NAME}"
  echo "git clone github.com/${GITHUB_REPO_NAME}"
  exit 1
fi

echo "::group::Verify replay values are available"
if [[ ! -f $GITHUB_REPO_NAME/.cookiecutter_replay/repo.json ]]; then
  echo "Failed to find stored values for Cookiecutter. Cannot continue"
  exit 1
fi
echo "::endgroup::"

echo "::group::Setting up Variables"
# Variables
TARGET_OWNER=$( echo "${GITHUB_REPO_NAME}" | awk -F'/' '{print $1;}' )
TARGET_NAME=$( echo "${GITHUB_REPO_NAME}" | awk -F'/' '{print $2;}' )
COOKIECUTTER_REPLAY_DIR="${TARGET_NAME}/.cookiecutter_replay"
echo "::endgroup::"

# Disable the pre/post hooks in this case. We just want to render the templates
echo "::group::Disabling pre/post hooks"
find RedVentures/repo/hooks -type f -exec mv -v {} {}.disable \;
echo "::endgroup::"

# Go into the owner level and create the cookiecutter configuration
echo "::group::Creating Cookiecutter Config & Setting Defaults"
cd $GITHUB_REPO_NAME/../ 
echo -e "replay_dir: $COOKIECUTTER_REPLAY_DIR" > $COOKIECUTTER_CONF
jq -s '.[0] * .[1].cookiecutter | { cookiecutter: . }' repo/cookiecutter.json ${TARGET_NAME}/.cookiecutter_replay/repo.json > ${TARGET_NAME}/.cookiecutter_replay/repo.merge.json
mv -f ${TARGET_NAME}/.cookiecutter_replay/repo.merge.json ${TARGET_NAME}/.cookiecutter_replay/repo.json
echo "::endgroup::"

# Run Cookiecutter
echo "::group::Render Template"
cookiecutter --replay -v -f ../RedVentures/repo --config-file $COOKIECUTTER_CONF
echo "::endgroup::"

echo "::group::What's Changed?"
cd $TARGET_NAME
git status
echo "::endgroup::"

echo "::group::Creating branch for changes"
if git checkout -b feature/airflow-template_upgrade_$(date +%Y-%m-%d); then
  git add .
  git config --global user.email "cdm-data-team@redventures.com"
  git config --global user.name "RedVentures/@cdm"
  git commit -m "Upgraded airflow-template to current"
  git push origin HEAD
  RES_BRANCH_PUSH=$?
fi
echo "::endgroup::"

# TODO: Create the related PR
echo "::group::Creating PR"
if which gh &>/dev/null; then
  if [[ $RES_BRANCH_PUSH -eq 0 ]]; then
    gh pr create -t "${PR_TITLE}" -b "${PR_BODY}" -B develop
  fi
else
  echo "Missing gh CLI. Cannot create the PR"
  exit 1
fi
echo "::endgroup::"
