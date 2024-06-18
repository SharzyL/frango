import asyncio
import json
from pathlib import Path
from typing import Dict, Type, Iterable, Optional

import grpc.aio as grpc
from loguru import logger
import rraft

from frango.config import Config
from frango.pb import node_pb, node_grpc
from frango.pb.generated.node_pb2 import QueryReq, QueryResp
from frango.sql_adaptor import SQLDef
from frango.table_def import Article, User, Read

from frango.node.sql_schedule import (
    Scheduler, sql_parse_one,
    SerialExecutionPlan, LocalExecutionPlan, DistributedExecutionPlan, ExecutionPlan, sql_to_str,
)
from frango.node.consensus import NodeConsensus, Proposal
from frango.node.storage import StorageBackend


class FrangoNode:
    class FrangoNodeServicer(node_grpc.FrangoNodeServicer):
        def __init__(self, node: "FrangoNode", event_loop: asyncio.AbstractEventLoop):
            super().__init__()
            self.node = node
            self.event_loop = event_loop

        def Ping(self, request: node_pb.Empty, context: grpc.ServicerContext) -> node_pb.PingResp:
            return node_pb.PingResp(id=self.node.node_id, leader_id=self.node.consensus.leader_id())

        def RRaft(self, request: node_pb.RRaftMessage, context: grpc.ServicerContext) -> node_pb.Empty:
            msg = rraft.Message.decode(request.bytes)
            self.node.consensus.on_receive_msg(msg, self.event_loop)
            return node_pb.Empty()

        def Query(self, request: node_pb.QueryReq, context: grpc.ServicerContext) -> node_pb.QueryResp:
            plan = self.node.scheduler.schedule_query(request.query_str)
            result = self.node.execute_plan(plan, must_local=False)
            result_rows_in_json: Iterable[str] = map(json.dumps, result)
            return node_pb.QueryResp(success=True, rows_in_json=result_rows_in_json)

        def SubQuery(self, request: node_pb.QueryReq, context: grpc.ServicerContext) -> node_pb.QueryResp:
            # SubQuery must be totally local
            query = sql_parse_one(request.query_str)
            plan = LocalExecutionPlan(query=query)
            result = self.node.execute_plan(plan, must_local=True)
            result_rows_in_json: Iterable[str] = map(json.dumps, result)
            return node_pb.QueryResp(success=True, rows_in_json=result_rows_in_json)

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

    def __init__(self, self_node_id: int, config: Config, known_classes: Optional[Dict[str, Type]] = None):
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
        self.known_classes = known_classes or dict()

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
        self.execute_plan(plan, must_local=True)

    @staticmethod
    def _parse_query_resp(resp: QueryResp) -> list[tuple]:
        assert resp.success  # handle error later

        def parse_row(row: str):
            row_ = json.loads(row)
            assert isinstance(row_, list)
            return tuple(row_)

        return list(map(parse_row, resp.rows_in_json))

    def execute_plan(self, plan: ExecutionPlan, must_local: bool = False) -> list[tuple]:
        if isinstance(plan, LocalExecutionPlan):
            return self.storage.execute(plan.query)

        elif isinstance(plan, DistributedExecutionPlan):
            assert not must_local
            result: list[tuple] = []

            # TODO: make it parallel
            # TODO: handle error
            for node_id, subquery in plan.queries_for_node.items():
                if node_id == self.node_id:
                    result += self.storage.execute(subquery)
                else:
                    query_req = QueryReq(query_str=sql_to_str(subquery))
                    resp: QueryResp = self.peer_stubs[node_id].SubQuery(query_req)
                    result += self._parse_query_resp(resp)
            return result

        elif isinstance(plan, SerialExecutionPlan):
            last_valid_result = []
            for step in plan.steps:
                result = self.execute_plan(step, must_local=must_local)
                if plan.result_cls is not None:
                    last_valid_result = result
            return last_valid_result

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
