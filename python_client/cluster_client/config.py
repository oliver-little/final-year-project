import logging
import sys

# Logging settings
logging.getLogger("cluster_client").setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.getLogger("cluster_client").addHandler(handler)

# Regex used for checking the names of keyspaces, tables or fields.
NAME_REGEX = r'^[A-Za-z0-9_]+$'

# Supported column types
VALID_COLUMN_TYPES = set(["timestamp", "bigint", "double", "text", "boolean"])

# Number of processes to use to perform uploads to cassandra
NUM_CASSANDRA_UPLOAD_PROCESSES = 4