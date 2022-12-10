from __future__ import annotations
from typing import Any

from cassandra.cluster import Cluster, Session, PreparedStatement

class CassandraConnector():
    def __init__(self, server_url : str = "localhost", port : int = None) -> None:
        self.cluster : Cluster = Cluster(server_url, port = port)
        self.session : Session = self.cluster.connect()     

    def has_keyspace(self, keyspace : str) -> bool:
        prep = self.session.prepare("SELECT * FROM system_schema.keyspaces WHERE keyspace_name='?'")
        res = self.session.execute(prep, [keyspace])
        return len(res) > 0

    def has_table(self, keyspace : str, table : str) -> bool:
        prep = self.session.prepare("SELECT ? FROM system_schema.tables WHERE keyspace_name='?';")
        res = self.session.execute(prep, [table, keyspace])
        return len(res) > 0

    def create_keyspace(self, keyspace : str) -> None:
        prep = self.session.prepare("CREATE KEYSPACE ? IF NOT EXISTS WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")
        self.session.execute(prep, [keyspace])
    
    def get_session(self) -> Session:
        return self.session