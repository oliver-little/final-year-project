from __future__ import annotations

import grpc
import protobuf.table_model_pb2 as protobuf_model
import protobuf.client_query_pb2_grpc as client_query

class ClientQueryManager():
    def __init__(self):
        self.grpc_channel = None
        self.stub = None

    def open():
        raise NotImplementedError("ClientQueryManager is a base class, so this is not implemented.")

    def close():
        raise NotImplementedError("ClientQueryManager is a base class, so this is not implemented.")

    def send_table(self, table : protobuf_model.Table) -> protobuf_model.TableComputeResult:
        response = self.stub.SendTable(table)
        return response

class InsecureClientQueryManager(ClientQueryManager):
    def __init__(self, address : str):
        super().__init__()
        self.address = address

    def open(self) -> InsecureClientQueryManager:
        if self.grpc_channel is None:
            self.grpc_channel = grpc.insecure_channel(self.address)
        self.stub = client_query.TableClientServiceStub(self.grpc_channel)
        return self
        
    def close(self) -> bool:
        try:
            self.grpc_channel.close()
            return True
        except AttributeError:
            print("Channel already closed.")
            return True

    def __enter__(self):
        return self.open()
    
    def __exit__(self, exception_type, exception_val, trace):
        return self.close()