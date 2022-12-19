from cluster_client.manager import ClusterManager
from cluster_client.model.field_expressions import *

if __name__ == "__main__":
    ClusterManager("localhost").cassandra_table("test", "forex_table").select(F("slug"), F("date")).select(F("slug")).evaluate()