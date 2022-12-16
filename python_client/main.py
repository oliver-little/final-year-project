from cluster_client.manager import ClusterManager

if __name__ == "__main__":
    ClusterManager("localhost").cassandra_table("test", "forex_table").evaluate()