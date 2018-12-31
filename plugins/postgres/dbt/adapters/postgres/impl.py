import psycopg2

import time

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.postgres import PostgresConnectionManager
import dbt.compat
import dbt.exceptions
import agate

from dbt.logger import GLOBAL_LOGGER as logger


GET_RELATIONS_MACRO_NAME = 'get_relations'


class PostgresAdapter(SQLAdapter):
    ConnectionManager = PostgresConnectionManager

    @classmethod
    def date_function(cls):
        return 'datenow()'

    def _link_cached_database_relations(self, database, schemas):
        table = self.execute_macro(GET_RELATIONS_MACRO_NAME, release=False)

        for (refed_schema, refed_name, dep_schema, dep_name) in table:
            referenced = self.Relation.create(
                database=database,
                schema=refed_schema,
                identifier=refed_name
            )
            dependent = self.Relation.create(
                database=database,
                schema=dep_schema,
                identifier=dep_name
            )

            # don't record in cache if this relation isn't in a relevant
            # schema
            if refed_schema.lower() in schemas:
                self.cache.add_link(dependent, referenced)

    def _link_cached_relations(self, manifest):
        schemas = manifest.get_used_schemas()
        # make a map of {db: [schemas]}
        schema_map = {}
        for db, schema in schemas:
            schema_map.setdefault(db, []).append(schema.lower())

        try:
            for db, schemas in schema_map.items():
                self._link_cached_database_relations(db, schemas)
        finally:
            self.release_connection(GET_RELATIONS_MACRO_NAME)

    def _relations_cache_for_schemas(self, manifest):
        super(PostgresAdapter, self)._relations_cache_for_schemas(manifest)
        self._link_cached_relations(manifest)
