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
        return pd.DataFrame(self.get_column_series())
    

    # Converts data into a dictionary of pandas series objects
    def get_column_series(self):
        # Convert data to columnar format
        columnar_data = [[] for _ in range(len(self.header.fields))]

        for row in self.rows:
            for index, value in enumerate(row.values):
                instance = value.WhichOneof("value")
                if instance == "null":
                    columnar_data[index].append(None)
                else:
                    columnar_data[index].append(getattr(value, instance))
        
        output = {}

        for index, header in enumerate(self.header.fields):
            if header.dataType == table_model_pb2.DataType.STRING:
                output[header.fieldName] = pd.Series(columnar_data[index], dtype="string")
            elif header.dataType == table_model_pb2.DataType.DATETIME:
                output[header.fieldName] = pd.to_datetime(pd.Series(columnar_data[index], dtype="string"), format="%Y-%m-%dT%H:%M:%S.%f%Z")
            elif header.dataType == table_model_pb2.DataType.INT:
                output[header.fieldName] = pd.Series(columnar_data[index], dtype="int")
            elif header.dataType == table_model_pb2.DataType.DOUBLE:
                output[header.fieldName] = pd.Series(columnar_data[index], dtype="double")
            elif header.dataType == table_model_pb2.DataType.BOOL:
                output[header.fieldName] = pd.Series(columnar_data[index], dtype="bool")
        return output
            
        


        