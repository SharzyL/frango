import asyncio
from typing import Dict

import grpc.aio as grpc
from loguru import logger
import rraft

from frango.node.consensus import NodeConsensus, Proposal
from frango.node.common import PeerConfig
from frango.pb import node_pb, node_grpc


class FrangoNode:
    class FrangoNodeServicer(node_grpc.FrangoNodeServicer):
        def __init__(self, node: "FrangoNode", event_loop: asyncio.AbstractEventLoop):
            super().__init__()
            self.node = node
            self.event_loop = event_loop

        def Ping(self, request: node_pb.Empty, context: grpc.ServicerContext):
            return node_pb.PingResp(id=1)

        def RRaft(self, request: node_pb.RRaftMessage, context: grpc.ServicerContext):
            msg = rraft.Message.decode(request.bytes)
            self.node.consensus.on_receive_msg(msg, self.event_loop)
            return node_pb.Empty()

    def _make_rraft_config(self) -> rraft.InMemoryRawNode:
        cfg = rraft.Config.default()
        cfg.set_election_tick(10)
        cfg.set_heartbeat_tick(3)
        cfg.set_id(self.peer_id)

        voters = list(self.peer_stubs.keys()) + [self.peer_id]
        cs = rraft.ConfState(voters=voters, learners=[])

        storage = rraft.MemStorage()
        storage.wl().set_conf_state(cs)

        raft_group = rraft.InMemoryRawNode(cfg, storage, rraft.default_logger())
        return raft_group

    def __init__(self, self_peer_id: int, peers_dict: Dict[int, PeerConfig]):
        self.grpc_server = grpc.server()
        self.peer_id = self_peer_id
        peer_self = peers_dict[self_peer_id]

        self.peer_stubs: Dict[int, node_grpc.FrangoNodeStub] = {
            peer_id: node_grpc.FrangoNodeStub(grpc.insecure_channel(peer.listen))
            for peer_id, peer in peers_dict.items()
            if peer_id != self_peer_id
        }

        self.consensus = NodeConsensus(self._make_rraft_config(), self.peer_stubs)

        self.listen = peer_self.listen

    async def loop(self):
        servicer = self.FrangoNodeServicer(self, asyncio.get_running_loop())
        node_grpc.add_FrangoNodeServicer_to_server(servicer, self.grpc_server)
        self.grpc_server.add_insecure_port(self.listen)
        await self.grpc_server.start()
        logger.info(f"grpc server started, listening on {self.listen}")

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.consensus.loop_until_stopped())

            if self.peer_id == 1:  # the initial leader
                # testsuite
                for i in range(10, 20):
                    await self.consensus.propose(Proposal.normal(i, "init"))
                    logger.success(f"propose {i} done")

    def bulk_load(self):
        pass

    def query(self):
        pass

    def update(self):
        pass
