
class AdapterPlugin(object):
    """Defines the basic requirements for a dbt adapter plugin.

    TODO: should we load the project config and extract the project name?

    :param type adapter: An adapter class, derived from BaseAdapter
    :param type credentials: A credentials object, derived from Credentials
    :param str project_name: The name of this adapter plugin's associated dbt
        project.
    :param str include_path: The path to this adapter plugin's root
    :param Optional[List[str]] dependencies: A list of adapter names that this\
        adapter depends upon.
    """
    def __init__(self, adapter, credentials, project_name, include_path,
                 dependencies=None):
        self.adapter = adapter
        self.credentials = credentials
        self.project_name = project_name
        self.include_path = include_path
        if dependencies is None:
            dependencies = []
        self.dependencies = dependencies
