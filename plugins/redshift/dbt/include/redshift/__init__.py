import os
from dbt.include.postgres import PROJECT_NAME as POSTGRES_PROJECT_NAME
from dbt.include.postgres import PACKAGE_PATH as POSTGRES_PACKAGE_PATH
PACKAGE_PATH = os.path.dirname(os.path.dirname(__file__))
PROJECT_NAME = 'dbt_redshift'

PACKAGES = {
    POSTGRES_PROJECT_NAME: POSTGRES_PACKAGE_PATH
}
