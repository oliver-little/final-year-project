from cluster_client.upload.cassandra import CassandraUploadHandler
from cluster_client.connector.cassandra import CassandraConnector

CassandraUploadHandler(CassandraConnector(["localhost"], 9042)).create_from_csv(r"G:\Documents\.Uni\.Y4\Project\forex.csv", "test", "forex_table", ["slug", "date"])