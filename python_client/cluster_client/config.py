# Regex used for checking the names of keyspaces, tables or fields.
NAME_REGEX = r'^[A-Za-z0-9_]+$'

# Supported column types
VALID_COLUMN_TYPES = set(["timestamp", "bigint", "double", "text", "boolean"])