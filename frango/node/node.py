import asyncio
from typing import Dict

import grpc.aio as grpc
from loguru import logger
from rraft import Message as rraft_Message

from frango.node.consensus import NodeConsensus
from frango.node.common import PeerConfig
from frango.pb import node_pb, node_grpc


class FrangoNode:
    class FrangoNodeServicer(node_grpc.FrangoNodeServicer):
        def __init__(self, node: "FrangoNode"):
            super().__init__()
            self.node = node

        def Ping(self, request: node_pb.Empty, context: grpc.ServicerContext):
            return node_pb.PingResp(id=1)

        def RRaft(self, request: node_pb.RRaftMessage, context: grpc.ServicerContext):
            msg = rraft_Message.decode(request.bytes)
            self.node.consensus.receive_message(msg)
            return node_pb.Empty()

    def __init__(self, self_peer_id: int, peers_dict: Dict[int, PeerConfig]):
        self.grpc_server = grpc.server()
        self.peer_id = self_peer_id
        peer_self = peers_dict[self_peer_id]

        self.peer_stubs: Dict[int, node_grpc.FrangoNodeStub] = {
            peer_id: node_grpc.FrangoNodeStub(grpc.insecure_channel(peer.listen))
            for peer_id, peer in peers_dict.items()
            if peer_id != self_peer_id
        }

        self.consensus = (
            NodeConsensus.create_raft_leader(self_peer_id, self.peer_stubs)
            if self_peer_id == 1
            else NodeConsensus.create_raft_follower(self.peer_stubs)
        )
        node_grpc.add_FrangoNodeServicer_to_server(self.FrangoNodeServicer(self), self.grpc_server)

        self.listen = peer_self.listen

    async def loop(self):
        self.grpc_server.add_insecure_port(self.listen)
        await self.grpc_server.start()
        consensus_loop_handler = asyncio.create_task(self.consensus.loop_until_stopped())
        if self.peer_id == 1:  # the initial leader
            await self.consensus.add_peers()

        await consensus_loop_handler

    def bulk_load(self):
        pass

    def query(self):
        pass

    def update(self):
        pass
