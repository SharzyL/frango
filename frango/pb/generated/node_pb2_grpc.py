# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import frango.pb.generated.node_pb2 as node__pb2


class FrangoNodeStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Ping = channel.unary_unary(
                '/frango.FrangoNode/Ping',
                request_serializer=node__pb2.Empty.SerializeToString,
                response_deserializer=node__pb2.PingResp.FromString,
                )
        self.RRaft = channel.unary_unary(
                '/frango.FrangoNode/RRaft',
                request_serializer=node__pb2.RRaftMessage.SerializeToString,
                response_deserializer=node__pb2.Empty.FromString,
                )
        self.Query = channel.unary_unary(
                '/frango.FrangoNode/Query',
                request_serializer=node__pb2.QueryReq.SerializeToString,
                response_deserializer=node__pb2.QueryResp.FromString,
                )
        self.SubQuery = channel.unary_unary(
                '/frango.FrangoNode/SubQuery',
                request_serializer=node__pb2.QueryReq.SerializeToString,
                response_deserializer=node__pb2.QueryResp.FromString,
                )
        self.SubQueryComplete = channel.unary_unary(
                '/frango.FrangoNode/SubQueryComplete',
                request_serializer=node__pb2.SubQueryCompleteReq.SerializeToString,
                response_deserializer=node__pb2.Empty.FromString,
                )


class FrangoNodeServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Ping(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RRaft(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Query(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SubQuery(self, request, context):
        """used for internode communication
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SubQueryComplete(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_FrangoNodeServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Ping': grpc.unary_unary_rpc_method_handler(
                    servicer.Ping,
                    request_deserializer=node__pb2.Empty.FromString,
                    response_serializer=node__pb2.PingResp.SerializeToString,
            ),
            'RRaft': grpc.unary_unary_rpc_method_handler(
                    servicer.RRaft,
                    request_deserializer=node__pb2.RRaftMessage.FromString,
                    response_serializer=node__pb2.Empty.SerializeToString,
            ),
            'Query': grpc.unary_unary_rpc_method_handler(
                    servicer.Query,
                    request_deserializer=node__pb2.QueryReq.FromString,
                    response_serializer=node__pb2.QueryResp.SerializeToString,
            ),
            'SubQuery': grpc.unary_unary_rpc_method_handler(
                    servicer.SubQuery,
                    request_deserializer=node__pb2.QueryReq.FromString,
                    response_serializer=node__pb2.QueryResp.SerializeToString,
            ),
            'SubQueryComplete': grpc.unary_unary_rpc_method_handler(
                    servicer.SubQueryComplete,
                    request_deserializer=node__pb2.SubQueryCompleteReq.FromString,
                    response_serializer=node__pb2.Empty.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'frango.FrangoNode', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class FrangoNode(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Ping(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/frango.FrangoNode/Ping',
            node__pb2.Empty.SerializeToString,
            node__pb2.PingResp.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RRaft(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/frango.FrangoNode/RRaft',
            node__pb2.RRaftMessage.SerializeToString,
            node__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Query(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/frango.FrangoNode/Query',
            node__pb2.QueryReq.SerializeToString,
            node__pb2.QueryResp.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SubQuery(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/frango.FrangoNode/SubQuery',
            node__pb2.QueryReq.SerializeToString,
            node__pb2.QueryResp.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SubQueryComplete(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/frango.FrangoNode/SubQueryComplete',
            node__pb2.SubQueryCompleteReq.SerializeToString,
            node__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
