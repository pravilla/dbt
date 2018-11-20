from dbt.adapters.snowflake.connections import SnowflakeConnectionManager
from dbt.adapters.snowflake.connections import SnowflakeCredentials
from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.impl import SnowflakeAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import snowflake

Plugin = AdapterPlugin(
    adapter=SnowflakeAdapter,
    credentials=SnowflakeCredentials,
    project_name=snowflake.PROJECT_NAME,
    include_path=snowflake.PACKAGE_PATH)
