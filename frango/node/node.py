from __future__ import annotations

import asyncio
from asyncio import Queue
from pathlib import Path
from typing import (Dict, Tuple, Optional, TypeAlias, Union, Any, TypeVar,
                    ParamSpec, Awaitable, Iterable, Sequence, Callable)

import grpc.aio as grpc
from loguru import logger
import rraft

from frango.config import Config
from frango.pb import node_pb, node_grpc
from frango.sql_adaptor import SQLDef
from frango.table_def import Article, User, Read

from frango.sql_adaptor import sql_to_str, sql_parse_one
from frango.node.scheduler import (
    Scheduler, SerialExecutionPlan, LocalExecutionPlan, DistributedExecutionPlan, ExecutionPlan,
)
from frango.node.consensus import NodeConsensus
from frango.node.storage import StorageBackend, ExecutionResult

Action: TypeAlias = node_pb.SubQueryCompleteReq.Action
ExecutorMessage: TypeAlias = Tuple[Union[ExecutionPlan, Action], Queue[ExecutionResult]]

F = TypeVar('F', bound=Callable[..., Any])
P = ParamSpec('P')
T = TypeVar('T')


class FrangoNode:
    class FrangoNodeServicer(node_grpc.FrangoNodeServicer):
        def __init__(self, node: FrangoNode, event_loop: asyncio.AbstractEventLoop,
                     executor_chan: Queue[ExecutorMessage]):
            super().__init__()
            self.node = node
            self.event_loop = event_loop
            self.executor_chan = executor_chan

        async def _execute_query(self, plan: ExecutionPlan) -> ExecutionResult:
            result_queue: Queue[ExecutionResult] = Queue(maxsize=1)

            await self.executor_chan.put((plan, result_queue))
            return await result_queue.get()

        @staticmethod
        def catch_request(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                try:
                    res = await func(*args, **kwargs)
                    return res
                except Exception as e:
                    logger.exception(f'Error on handling {func.__name__}')
                    raise e

            return wrapper

        @catch_request
        async def Ping(self, request: node_pb.Empty, context: grpc.ServicerContext) -> node_pb.PingResp:
            return node_pb.PingResp(id=self.node.node_id, leader_id=self.node.consensus.leader_id())

        @catch_request
        async def RRaft(self, request: node_pb.RRaftMessage, context: grpc.ServicerContext) -> node_pb.Empty:
            msg = rraft.Message.decode(request.the_bytes)
            self.node.consensus.on_receive_msg(msg, self.event_loop)
            return node_pb.Empty()

        async def Query(self, request: node_pb.QueryReq, context: grpc.ServicerContext) -> node_pb.QueryResp:
            try:
                plan = self.node.scheduler.schedule_query(request.query_str)
                result = await self._execute_query(plan)
                return result.to_pb()
            except Exception as e:
                logger.exception(f'Error on handling query `{request.query_str}`: {repr(e)}')
                return node_pb.QueryResp(err_msg=str(e), is_error=True)

        @catch_request
        async def SubQuery(self, request: node_pb.QueryReq, context: grpc.ServicerContext) -> node_pb.QueryResp:
            # SubQuery must be totally local
            query = sql_parse_one(request.query_str)
            plan = LocalExecutionPlan(query=query)
            result = await self._execute_query(plan)
            return result.to_pb()

        @catch_request
        async def SubQueryComplete(self, request: node_pb.SubQueryCompleteReq,
                                   context: grpc.ServicerContext) -> node_pb.Empty:
            action = request.action
            result_queue: Queue[ExecutionResult] = Queue(maxsize=1)
            await self.executor_chan.put((action, result_queue))
            await result_queue.get()
            return node_pb.Empty()

    def _make_rraft_config(self) -> rraft.InMemoryRawNode:
        cfg = rraft.Config.default()
        cfg.set_election_tick(10)
        cfg.set_heartbeat_tick(3)
        cfg.set_id(self.node_id)

        voters = list(self.peer_stubs.keys()) + [self.node_id]

        cs = rraft.ConfState(voters=voters, learners=[])  # type: ignore[abstract]

        storage = rraft.MemStorage()
        storage.wl().set_conf_state(cs)

        raft_group = rraft.InMemoryRawNode(cfg, storage, rraft.default_logger())
        return raft_group

    def __init__(self, self_node_id: int, config: Config, known_classes: Optional[Dict[str, type]] = None):
        self.grpc_server: grpc.Server = grpc.server()
        self.node_id: int = self_node_id

        peers_dict = {peer.node_id: peer for peer in config.peers}
        peer_self = peers_dict[self_node_id]

        self.peer_stubs: Dict[int, node_grpc.FrangoNodeStub] = {
            node_id: node_grpc.FrangoNodeStub(grpc.insecure_channel(peer.listen))  # type: ignore[no-untyped-call]
            for node_id, peer in peers_dict.items()
            if node_id != self_node_id
        }

        self.known_classes: Dict[str, type] = known_classes or dict()
        self.consensus: NodeConsensus = NodeConsensus(self._make_rraft_config(), self.peer_stubs, config.raft)
        self.scheduler: Scheduler = Scheduler(config.partitions, node_id_list=list(peers_dict.keys()),
                                              known_classes=self.known_classes)

        db_path = Path(config.db_path_pattern.replace('{{node_id}}', str(self_node_id)))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.storage = StorageBackend(db_path)

        self.listen: str = peer_self.listen

        self._stop_chan: Queue[None] = Queue(maxsize=1)

    async def bulk_load(self, table_dat_files: Dict[str, Path]) -> None:
        known_tables = [Article, User, Read]
        known_table_idx = {cls.__name__: cls for cls in known_tables}
        tables: Dict[str, Sequence[SQLDef]] = dict()
        for table_name, dat_file in table_dat_files.items():
            cls = known_table_idx[table_name]
            table = []
            logger.info(f'reading "{cls.__name__}" data from "{dat_file}"')
            with open(dat_file, 'r') as f:
                for line in f:
                    assert hasattr(cls, 'from_json')
                    table.append(cls.from_json(line))
            tables[table_name] = table

        plan = self.scheduler.schedule_bulk_load_for_node(tables, self.node_id)
        await self._execute_plan(plan)

    async def _finalize(self, is_error: bool, nodes: Iterable[int]) -> None:
        for node_id_ in nodes:
            if node_id_ == self.node_id:  # local node
                if is_error:
                    self.storage.rollback()
                else:
                    self.storage.commit()
            else:
                action = Action.ABORT if is_error else Action.COMMIT
                await self.peer_stubs[node_id_].SubQueryComplete(node_pb.SubQueryCompleteReq(action=action))

    async def executor_loop(self, stop_chan: Queue[None], query_chan: Queue[ExecutorMessage]) -> None:
        while stop_chan.empty():
            execute, result_chan = await query_chan.get()
            if isinstance(execute, ExecutionPlan):
                result = await self._execute_plan(execute)
                await result_chan.put(result)
            else:
                await self._finalize(execute == Action.ABORT, (self.node_id,))
                await result_chan.put(ExecutionResult())  # empty response

    async def _execute_distributed_plan(self, plan: DistributedExecutionPlan) -> ExecutionResult:
        # 1. make a raft propose
        # 2. wait for propose commit
        # 3. distribute subquery
        # 4. check (1) propose success (2) subquery success
        # 5. commit / rollback
        raise NotImplementedError

    # Note: this is not thread safe, hence we need a loop
    async def _execute_plan(self, plan: ExecutionPlan) -> ExecutionResult:
        if isinstance(plan, LocalExecutionPlan):
            local_result = self.storage.execute(plan.query)
            if plan.auto_commit:
                await self._finalize(local_result.is_error, (self.node_id,))
            return local_result

        elif isinstance(plan, DistributedExecutionPlan):
            if len(plan.queries_for_node) == 0:
                return ExecutionResult()  # empty response for empty plan

            distri_result: Optional[ExecutionResult] = None

            # TODO: make it parallel
            for node_id, subquery in plan.queries_for_node.items():
                new_result: Optional[ExecutionResult] = None

                # execute
                if node_id == self.node_id:
                    new_result = self.storage.execute(subquery)
                else:
                    query_req = node_pb.QueryReq(query_str=sql_to_str(subquery))
                    resp: node_pb.QueryResp = await self.peer_stubs[node_id].SubQuery(query_req)
                    new_result = ExecutionResult.from_pb(resp)

                assert new_result is not None
                if distri_result is None:
                    distri_result = new_result
                else:
                    distri_result.merge(new_result)

            assert distri_result is not None  # it must be non-null when plan is not empty
            if plan.auto_commit:
                await self._finalize(distri_result.is_error, plan.queries_for_node.keys())

            return distri_result

        elif isinstance(plan, SerialExecutionPlan):
            if len(plan.steps) == 0:
                return ExecutionResult()  # empty response for empty plan

            involved_nodes: set[int] = set()

            serial_result: Optional[ExecutionResult] = None
            for step in plan.steps:
                serial_result = await self._execute_plan(step)

                if isinstance(step, LocalExecutionPlan):
                    involved_nodes.add(self.node_id)
                elif isinstance(step, DistributedExecutionPlan):
                    involved_nodes.update(step.queries_for_node.keys())
                else:
                    assert False, f'not knowing how to determine involved nodes for {step.__class__.__name__}'

                assert serial_result is not None
                if serial_result.is_error:
                    break

            assert serial_result is not None  # it must be non-null when plan is not empty

            if plan.auto_commit:
                await self._finalize(serial_result.is_error, involved_nodes)

            return serial_result

        else:
            raise NotImplementedError(f'{plan.__class__.__name__} is not supported')

    async def loop(self) -> None:
        executor_chan: Queue[ExecutorMessage] = Queue()

        servicer = self.FrangoNodeServicer(self, asyncio.get_running_loop(), executor_chan)
        node_grpc.add_FrangoNodeServicer_to_server(servicer, self.grpc_server)  # type: ignore[no-untyped-call]
        self.grpc_server.add_insecure_port(self.listen)
        await self.grpc_server.start()
        logger.info(f"grpc server started, listening on {self.listen}")

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.consensus.loop_until_stopped())
            tg.create_task(self.executor_loop(self._stop_chan, executor_chan))
