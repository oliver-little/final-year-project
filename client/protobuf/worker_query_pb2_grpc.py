# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import worker_query_pb2 as worker__query__pb2


class WorkerComputeServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ComputePartialResultCassandra = channel.unary_unary(
                '/WorkerComputeService/ComputePartialResultCassandra',
                request_serializer=worker__query__pb2.ComputePartialResultCassandraRequest.SerializeToString,
                response_deserializer=worker__query__pb2.ComputePartialResultCassandraResult.FromString,
                )


class WorkerComputeServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def ComputePartialResultCassandra(self, request, context):
        """Computes a partial result
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_WorkerComputeServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ComputePartialResultCassandra': grpc.unary_unary_rpc_method_handler(
                    servicer.ComputePartialResultCassandra,
                    request_deserializer=worker__query__pb2.ComputePartialResultCassandraRequest.FromString,
                    response_serializer=worker__query__pb2.ComputePartialResultCassandraResult.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'WorkerComputeService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class WorkerComputeService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def ComputePartialResultCassandra(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/WorkerComputeService/ComputePartialResultCassandra',
            worker__query__pb2.ComputePartialResultCassandraRequest.SerializeToString,
            worker__query__pb2.ComputePartialResultCassandraResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
