from airflow.plugins_manager import AirflowPlugin
from flask_admin.base import MenuLink

"""
Add the puckel GitHub repo to the docs directory
"""
puckel_docs_ml = MenuLink(
    category='Docs',
    name='Puckel GitHub',
    url='https://github.com/puckel/docker-airflow/'
)

circleci_job_ml = MenuLink(
    category='Docs',
    name='CircleCI',
    url='https://circleci.com/gh/RedVentures/cards-airflow'
)

github_repo_ml = MenuLink(
    category='Docs',
    name='DAG Repo',
    url='https://github.com/RedVentures/cards-airflow'
)


class PuckelLinksPlugin(AirflowPlugin):
    name = 'puckel_menu_links'
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    menu_links = [puckel_docs_ml, circleci_job_ml, github_repo_ml]
    appbuilder_views = []
    appbuilder_menu_items = [
        {
            "name": ml.name,
            "category": ml.category,
            "href": ml.url,
        } for ml in menu_links
    ]
