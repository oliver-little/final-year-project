import pandas as pd

import cluster_client.protobuf.table_model_pb2 as table_model_pb2

class StreamedTableResultBuilder():
    def __init__(self, iterator):
        self.header = None
        self.rows = []
        for result in iterator:
            if result.HasField("header"):
                if self.header is not None:
                    raise ValueError("Header sent twice by remote gRPC server.")
                else:
                    self.header = result.header
            else:
                self.rows.append(result.row)

    def get_header_protobuf(self):
        return self.header

    def get_rows_protobuf(self):
        return self.rows

    def get_dataframe(self):
        raise NotImplementedError("Not implemented yet")        
        