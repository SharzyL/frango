import asyncio
from pathlib import Path
from typing import Dict

import grpc.aio as grpc
from loguru import logger
import rraft

from frango.config import Config
from frango.pb import node_pb, node_grpc
from frango.sql_adaptor import SQLDef
from frango.table_def import Article, User, Read

from frango.node.sql_schedule import Scheduler
from frango.node.consensus import NodeConsensus, Proposal
from frango.node.storage import StorageBackend


class FrangoNode:
    class FrangoNodeServicer(node_grpc.FrangoNodeServicer):
        def __init__(self, node: "FrangoNode", event_loop: asyncio.AbstractEventLoop):
            super().__init__()
            self.node = node
            self.event_loop = event_loop

        def Ping(self, request: node_pb.Empty, context: grpc.ServicerContext):
            return node_pb.PingResp(id=self.node.node_id, leader_id=self.node.consensus.leader_id())

        def RRaft(self, request: node_pb.RRaftMessage, context: grpc.ServicerContext):
            msg = rraft.Message.decode(request.bytes)
            self.node.consensus.on_receive_msg(msg, self.event_loop)
            return node_pb.Empty()

        def Query(self, request: node_pb.QueryReq, context: grpc.ServicerContext):
            resp: node_pb.QueryResp = node_pb.QueryResp()
            pass

    def _make_rraft_config(self) -> rraft.InMemoryRawNode:
        cfg = rraft.Config.default()
        cfg.set_election_tick(10)
        cfg.set_heartbeat_tick(3)
        cfg.set_id(self.node_id)

        voters = list(self.peer_stubs.keys()) + [self.node_id]
        cs = rraft.ConfState(voters=voters, learners=[])

        storage = rraft.MemStorage()
        storage.wl().set_conf_state(cs)

        raft_group = rraft.InMemoryRawNode(cfg, storage, rraft.default_logger())
        return raft_group

    def __init__(self, self_node_id: int, config: Config):
        self.grpc_server = grpc.server()
        self.node_id = self_node_id

        peers_dict = {peer.node_id: peer for peer in config.peers}
        peer_self = peers_dict[self_node_id]

        self.peer_stubs: Dict[int, node_grpc.FrangoNodeStub] = {
            node_id: node_grpc.FrangoNodeStub(grpc.insecure_channel(peer.listen))
            for node_id, peer in peers_dict.items()
            if node_id != self_node_id
        }

        self.consensus = NodeConsensus(self._make_rraft_config(), self.peer_stubs, config.raft)
        self.scheduler = Scheduler(config.partitions, node_id_list=list(peers_dict.keys()))

        db_path = Path(config.db_path_pattern.replace('{{node_id}}', str(self_node_id)))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.storage = StorageBackend(db_path)

        self.listen = peer_self.listen

    def bulk_load(self, table_dat_files: Dict[str, Path]) -> None:
        known_tables = [Article, User, Read]
        known_table_idx = {cls.__name__: cls for cls in known_tables}
        tables: Dict[str, list[SQLDef]] = dict()
        for table_name, dat_file in table_dat_files.items():
            cls = known_table_idx[table_name]
            table = []
            logger.info(f'reading "{cls.__name__}" data from "{dat_file}"')
            with open(dat_file, 'r') as f:
                for line in f:
                    table.append(cls.from_json(line))
            tables[table_name] = table

        plan = self.scheduler.schedule_bulk_load_for_node(tables, self.node_id)
        self.storage.execute(plan)

    async def loop(self) -> None:
        servicer = self.FrangoNodeServicer(self, asyncio.get_running_loop())
        node_grpc.add_FrangoNodeServicer_to_server(servicer, self.grpc_server)
        self.grpc_server.add_insecure_port(self.listen)
        await self.grpc_server.start()
        logger.info(f"grpc server started, listening on {self.listen}")

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.consensus.loop_until_stopped())

            if self.node_id == 1:  # the initial leader
                # testsuite
                for i in range(10, 20):
                    await self.consensus.propose(Proposal.normal(i, "init"))
                    logger.success(f"propose {i} done")

    def query(self):
        pass

    def update(self):
        pass
