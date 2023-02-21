from __future__ import annotations
from typing import List

import grpc

from cluster_client.connector.cluster import ClusterConnector
from cluster_client.model.table import Table
from cluster_client.model.data_source import CassandraDataSource


# ClusterManager("address").cassandraTable().

class ClusterManager():
    def __init__(self, address : str, port : int = 50051, credentials : grpc.ChannelCredentials = None) -> None:
        self.address = address
        self.port = port
        self.connector = ClusterConnector(address, str(port), credentials)
        self.connector.open()

    def cassandra_table(self, keyspace : str, table : str):
        return Table(self.connector, CassandraDataSource(keyspace, table, self.address), [])