from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.bigquery.connections import BigQueryCredentials
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.impl import BigQueryAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import bigquery

Plugin = AdapterPlugin(
    adapter=BigQueryAdapter,
    credentials=BigQueryCredentials,
    project_name=bigquery.PROJECT_NAME,
    include_path=bigquery.PACKAGE_PATH)
