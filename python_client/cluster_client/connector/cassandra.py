from __future__ import annotations
import re
from cassandra.cluster import Cluster, Session, PreparedStatement
from cassandra.auth import AuthProvider, PlainTextAuthProvider

from cluster_client.config import NAME_REGEX

class CassandraConnector():
    def __init__(self, server_url : str = ["localhost"], port : int = 9042, username : str = None, password : str = None, auth_provider : AuthProvider = None) -> None:
        if isinstance(server_url, str):
            server_url = [server_url]

        if auth_provider is not None:
            self.auth_provider = auth_provider
            self.cluster : Cluster = Cluster(server_url, port = port, auth_provider=self.auth_provider)
        elif username is not None and password is not None:
            self.auth_provider = PlainTextAuthProvider(username=username, password=password)
            self.cluster : Cluster = Cluster(server_url, port = port, auth_provider=self.auth_provider)
        else:
            self.auth_provider = None
            self.cluster : Cluster = Cluster(server_url, port = port)

        self.server_url = server_url
        self.port = port
        self.session : Session = self.cluster.connect()
        self.session.request_timeout = 60     

    def has_keyspace(self, keyspace : str) -> bool:
        if not re.match(NAME_REGEX, keyspace):
            raise ValueError("Keyspace must be alphanumeric")

        res = self.session.execute(f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name='{keyspace}'")
        if res:
            return True
        else:
            return False

    def has_table(self, keyspace : str, table : str) -> bool:
        if not re.match(NAME_REGEX, keyspace):
            raise ValueError("Keyspace must be alphanumeric")
        elif not re.match(NAME_REGEX, table):
            raise ValueError("Table must be alphanumeric")
        query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace}' AND table_name='{table}';"
        res = self.session.execute(query)
        if res.one():
            return True
        else:
            return False

    def create_keyspace(self, keyspace : str) -> None:
        if not re.match(NAME_REGEX, keyspace):
            raise ValueError("Keyspace must be alphanumeric")
        self.session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}};")
    
    def get_session(self) -> Session:
        return self.session