from airflow.plugins_manager import AirflowPlugin
from flask_admin.base import MenuLink

try:
    from rvairflow.version import __version__ as rvairflow_version
except ImportError:
    rvairflow_version = None

try:
    from airflow import __version__ as airflow_version
except ImportError:
    airflow_version = None

try:
    from flask_appbuilder import __version__ as fab_version
except ImportError:
    fab_version = None

"""
Unified list of Document links to be added
"""

cat = 'Docs'
gh_url = 'https://github.com'
astro_url = 'https://www.astronomer.io'

# RV-specific links
image_gh = MenuLink(category=cat, name='Github: rvairflow', url=f'{gh_url}/RedVentures/rvairflow')
circleci = MenuLink(category=cat, name='CircleCI', url='https://circleci.com/gh/RedVentures/cards-airflow')
repo = MenuLink(category=cat, name='DAG Repo', url=f'{gh_url}/RedVentures/cards-airflow')

# Astronomer Links
astro_plugins = MenuLink(category=cat, name='Astronomer: plugins', url=f'{gh_url}/airflow-plugins')
astro_docs = MenuLink(category=cat, name='Astronomer: docs', url=f'{astro_url}/docs')
astro_guides = MenuLink(category=cat, name='Astronomer: guides', url=f'{astro_url}/guides')

# Versions
rvairflow_ml = MenuLink(category='About', name=f'rvairflow: {rvairflow_version}', url='#')
airflow_ml = MenuLink(category='About', name=f'airflow: {airflow_version}', url='#')
fab_ml = MenuLink(category='About', name=f'FAB: {fab_version}', url='#')


class DocLinksPlugin(AirflowPlugin):
    name = 'doc_menu_links'
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    menu_links = [
      image_gh, circleci, repo,
      astro_plugins, astro_docs, astro_guides,
      rvairflow_ml, airflow_ml, fab_ml
    ]
    appbuilder_views = []
    appbuilder_menu_items = [
        {
            "name": ml.name,
            "category": ml.category,
            "category_icon": "fa-rocket",
            "href": ml.url,
        } for ml in menu_links
    ]
