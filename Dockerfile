
FROM redventures-cdm-docker.jfrog.io/rvairflow:1.10.14

# Create a new entrypoint_wrapper to manage DB init and upgrades for Airflow
# COPY bin/entrypoint_wrapper.sh /entrypoint_wrapper

# Install a specific version of tini regardless of the parent build (https://github.com/krallin/tini)
USER root
ENV TINI_VERSION v0.18.0
ENV TINI_ARCH "-amd64"
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini${TINI_ARCH} /tini
RUN chmod +x /tini

# TODO: Add signal handlers to elegantly upgrade worker nodes
USER airflow

# Add in any packages from requirements.txt
# TODO: Bundle in some pypi configurations to access the rv-airflow setup
ARG ARTIFACTORY_USER
ARG ARTIFACTORY_TOKEN
COPY requirements.txt .
RUN mkdir ${HOME}/.pip; \
  echo "[global]\nextra-index-url = https://${ARTIFACTORY_USER}:${ARTIFACTORY_TOKEN}@redventures.jfrog.io/redventures/api/pypi/rv-airflow/simple\n" \ 
  > ${HOME}/.pip/pip.conf
RUN pip install -r requirements.txt

ENTRYPOINT ["/tini", "--", "/entrypoint_wrapper"]
