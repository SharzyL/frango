import asyncio
from asyncio import Queue
import re
from collections import deque
from typing import Dict, List, Optional, Tuple, Deque

import grpc.aio as grpc
from grpc import StatusCode as grpc_status

from rraft import (
    ConfChange,
    Config,
    Entry,
    EntryType,
    Logger,
    MemStorage,
    Message,
    MessageType,
    InMemoryRawNode,
    Snapshot,
    StateRole, ConfChangeType, default_logger, LightReady, Ready
)
from loguru import logger

from frango.node.common import TICK_SECONDS, LEADER_ID
from frango.pb import node_pb, node_grpc


def _rraft_logger() -> Logger:
    return default_logger()


class Proposal:
    def __init__(
            self,
            *,
            normal: Optional[Tuple[int, str]] = None,
            conf_change: Optional[ConfChange] = None,
            transfer_leader: Optional[int] = None,
    ):
        self.normal = normal
        self.conf_change = conf_change
        self.transfer_leader = transfer_leader
        self._propose_success: Queue[bool] = Queue(maxsize=1)
        self.proposed: Optional[int] = None  # Optional int of index in log

    def send_success_signal(self, is_success: bool):
        if not self._propose_success.full():
            self._propose_success.put_nowait(is_success)

    async def wait_until_success(self) -> bool:
        return await self._propose_success.get()

    @staticmethod
    def conf_change(cc: ConfChange) -> "Proposal":
        return Proposal(
            conf_change=cc.clone(),
        )

    @staticmethod
    def normal(key: int, value: str) -> "Proposal":
        return Proposal(
            normal=(key, value),
        )


def _default_config() -> Config:
    cfg = Config.default()
    cfg.set_election_tick(10)
    cfg.set_heartbeat_tick(3)
    return cfg


PeerStubs = Dict[int, node_grpc.FrangoNodeStub]


class NodeConsensus:
    def __init__(self, raft_group: Optional[InMemoryRawNode], peer_stubs: PeerStubs) -> None:
        self.raft_group = raft_group
        self.kv_pairs = dict()

        self._peer_stubs = peer_stubs
        self._proposals: Deque[Proposal] = deque()
        self._mailbox: Queue[Message] = Queue()
        self._stop_chan: Queue[None] = Queue(maxsize=1)
        self._on_ready_cond = asyncio.Condition()
        self._become_leader_cond = asyncio.Condition()
        self._raft_state_lock = asyncio.Lock()

    def send_stop_signal(self):
        if not self._stop_chan.full():
            self._stop_chan.put_nowait(None)

    @staticmethod
    def create_raft_leader(peer_stubs: PeerStubs) -> "NodeConsensus":
        cfg = _default_config()
        cfg.set_id(LEADER_ID)
        s = Snapshot.default()
        # Because we don't use the same configuration to initialize every node, so we use
        # a non-zero index to force new followers catch up logs by snapshot first, which will
        # bring all nodes to the same initial state.
        s.get_metadata().set_index(1)
        s.get_metadata().set_term(1)
        s.get_metadata().get_conf_state().set_voters([LEADER_ID])
        storage = MemStorage()
        storage.wl().apply_snapshot(s)
        raft_group = InMemoryRawNode(cfg, storage, _rraft_logger())
        return NodeConsensus(raft_group, peer_stubs)

    @staticmethod
    def create_raft_follower(peer_stubs: PeerStubs) -> "NodeConsensus":
        return NodeConsensus(None, peer_stubs)

    def _maybe_init_with_message(self, msg: Message) -> None:
        def is_initial_msg(msg_: Message) -> bool:
            msg_type = msg_.get_msg_type()
            return msg_type == MessageType.MsgRequestVote \
                or msg_type == MessageType.MsgRequestPreVote \
                or (msg_type == MessageType.MsgHeartbeat and msg_.get_commit() == 0)

        if self.raft_group is None:
            if is_initial_msg(msg):
                cfg = _default_config()
                cfg.set_id(msg.get_to())
                storage = MemStorage()
                self.raft_group = InMemoryRawNode(cfg, storage, _rraft_logger())
            else:
                return

    async def _send_messages(self, msgs: List[Message]) -> None:
        for msg in msgs:
            get_to = msg.get_to()
            try:
                if msg.get_msg_type() in (MessageType.MsgHeartbeat, MessageType.MsgHeartbeatResponse):
                    logger.trace(f"send heartbeat: {msg}")
                else:
                    logger.debug(f"sending msg to {get_to}: {msg}")
                peer_stub = self._peer_stubs[get_to]
                await peer_stub.RRaft(node_pb.RRaftMessage(bytes=msg.encode()))
            except grpc.AioRpcError as e:
                if e.code() == grpc_status.UNAVAILABLE:
                    logger.warning(f"peer {get_to} not available")
                else:
                    raise e

    async def _handle_committed_entries(self, committed_entries: List[Entry], store: MemStorage) -> None:
        for entry in committed_entries:
            logger.success(f'handle committed (index={entry.get_index()}, type={entry.get_entry_type()}, '
                           f'data={str(entry.get_data())})')
            if not entry.get_data():
                # From new elected leaders.
                continue

            if entry.get_entry_type() == EntryType.EntryNormal:
                # For normal proposals, extract the key-value pair and then
                # insert them into the kv engine.
                data = str(entry.get_data(), "utf-8")
                reg = re.compile(r"put ([0-9]+) (.+)")

                if caps := reg.match(data):
                    key, value = int(caps.group(1)), str(caps.group(2))
                    self.kv_pairs[key] = value

            elif entry.get_entry_type() == EntryType.EntryConfChange:
                cc = ConfChange.default()
                new_conf = ConfChange.decode(entry.get_data())

                cc.set_id(new_conf.get_id())
                cc.set_node_id(new_conf.get_node_id())
                cc.set_context(new_conf.get_context())
                cc.set_change_type(new_conf.get_change_type())

                cs = self.raft_group.apply_conf_change(cc)
                store.wl().set_conf_state(cs)

            # notify that the proposal succeeded
            if self.raft_group.get_raft().get_state() == StateRole.Leader:
                proposal = self._proposals.popleft()
                proposal.send_success_signal(True)

    def _raft_propose(self, proposal: Proposal) -> None:
        last_index1 = self.raft_group.get_raft().get_raft_log().last_index() + 1

        if proposal.normal:
            key, value = proposal.normal
            self.raft_group.propose(b'', bytes(f'put {key} {value}', "utf-8"))

        elif proposal.conf_change:
            self.raft_group.propose_conf_change(b'', proposal.conf_change.clone())

        elif proposal.transfer_leader:
            # TODO: implement transfer leader.
            raise NotImplementedError

        last_index2 = self.raft_group.get_raft().get_raft_log().last_index() + 1

        if last_index2 == last_index1:
            # Propose failed, don't forget to respond to the client.
            proposal.send_success_signal(False)
        else:
            proposal.proposed = last_index1

    async def _notify_ready(self):
        async with self._on_ready_cond:
            self._on_ready_cond.notify()

    async def _wait_ready_loop(self):
        while not self._stop_chan.full():
            async with self._on_ready_cond:
                await self._on_ready_cond.wait_for(lambda: self.raft_group and self.raft_group.has_ready())

            await self._on_ready()

    async def _on_ready(self) -> None:
        async with self._raft_state_lock:
            store = self.raft_group.get_raft().get_raft_log().get_store().clone()
            ready: Ready = self.raft_group.ready()

            if ready.messages():
                await self._send_messages(ready.take_messages())

            # Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
            snapshot_default = Snapshot.default()
            if ready.snapshot() != snapshot_default.make_ref():
                s = ready.snapshot().clone()
                store.wl().apply_snapshot(s)

            await self._handle_committed_entries(ready.take_committed_entries(), store)

            # Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
            # raft logs to the latest position.
            store.wl().append(ready.entries())

            if hs := ready.hs():
                store.wl().set_hardstate(hs)

            if persisted_msgs := ready.take_persisted_messages():
                await self._send_messages(persisted_msgs)

            # Call `RawNode::advance` interface to update position flags in the raft.

            light_rd: LightReady = self.raft_group.advance(ready.make_ref())
            # Update commit index.
            if commit := light_rd.commit_index():
                store.wl().hard_state().set_commit(commit)

            await self._send_messages(light_rd.take_messages())
            await self._handle_committed_entries(light_rd.take_committed_entries(), store)
            self.raft_group.advance_apply()

    def is_leader(self):
        if self.raft_group is None:
            return False
        else:
            return self.raft_group.get_raft().get_state() == StateRole.Leader

    async def add_peers(self):
        for peer_id, peer in self._peer_stubs.items():
            conf_change = ConfChange.default()
            conf_change.set_node_id(peer_id)
            conf_change.set_change_type(ConfChangeType.AddNode)
            await self.propose(Proposal(conf_change=conf_change))

    async def propose(self, proposal: Proposal) -> None:
        async with self._become_leader_cond:
            await self._become_leader_cond.wait_for(lambda: self.is_leader())
        async with self._raft_state_lock:
            self._proposals.append(proposal)
            self._raft_propose(proposal)
        await proposal.wait_until_success()

    def on_receive_msg(self, msg: Message, event_loop: asyncio.AbstractEventLoop) -> None:
        if msg.get_msg_type() in (MessageType.MsgHeartbeat, MessageType.MsgHeartbeatResponse):
            logger.trace(f"recv heartbeat: {msg}")
        else:
            logger.debug(f"recv msg from {msg.get_from()}: {msg}")

        self._maybe_init_with_message(msg)

        async def step():
            async with self._raft_state_lock:
                self.raft_group.step(msg)
            if self.raft_group.has_ready():
                await self._notify_ready()

        if self.raft_group is not None:
            event_loop.create_task(step())

    async def loop_until_stopped(self) -> None:
        wait_ready_loop = asyncio.create_task(self._wait_ready_loop())

        next_tick = asyncio.get_running_loop().time() + TICK_SECONDS  # tick immediately
        while not self._stop_chan.full():

            # wait until next tick
            await asyncio.sleep(next_tick - asyncio.get_running_loop().time())
            next_tick = asyncio.get_running_loop().time() + TICK_SECONDS
            if self.raft_group:
                async with self._raft_state_lock:
                    self.raft_group.tick()
                    async with self._become_leader_cond:
                        self._become_leader_cond.notify()

                # handle readies
                if self.raft_group.has_ready():
                    await self._notify_ready()

        await wait_ready_loop
