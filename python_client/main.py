from cluster_client.upload.cassandra import CassandraUploadHandler
from cluster_client.connector.cassandra import CassandraConnector

if __name__ == "__main__":
    try:
        upload_handler = CassandraUploadHandler(CassandraConnector(["localhost"], 9042))
        upload_handler.create_from_csv(r"D:\uni\Y4S1\Project\forex.csv", "test", "forex_table", ["slug", "date"])
    finally:
        upload_handler.connector.cluster.shutdown()