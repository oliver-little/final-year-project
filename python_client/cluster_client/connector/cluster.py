import grpc

import cluster_client.protobuf.client_query_pb2_grpc as client_query_pb2_grpc

class ClusterConnector():
    def __init__(self, address : str, port : int = 50051, credentials : grpc.ChannelCredentials = None) -> None:
        self.address = address
        self.port = port
        self.credentials = credentials

    def open(self) -> None:
        if self.credentials is not None:
            self.grpc_channel = grpc.secure_channel(self.address + ":" + str(self.port), self.credentials)
        else:
            self.grpc_channel = grpc.insecure_channel(self.address + ":" + str(self.port))

        self.table_client_service : client_query_pb2_grpc.TableClientServiceStub = client_query_pb2_grpc.TableClientServiceStub(self.grpc_channel)

    def close(self) -> None:
        if self.grpc_channel is not None:
            self.grpc_channel.close()
            self.table_client_service = None
    
    def __enter__(self) -> None:
        return self.open()

    def __exit__(self) -> None:
        return self.close()