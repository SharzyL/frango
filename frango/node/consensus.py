from __future__ import annotations

import asyncio
from asyncio import Queue
import re
from typing import Dict, List, Optional, Tuple

import grpc.aio as grpc
from grpc import StatusCode as grpcStatus

import rraft
from loguru import logger

from frango.pb import node_pb, node_grpc
from frango.config import Config

RaftConfig = Config.Raft


class Proposal:
    def __init__(
            self,
            *,
            normal: Optional[Tuple[int, str]] = None,
            conf_change: Optional[rraft.ConfChange] = None,
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
    def conf_change(cc: rraft.ConfChange) -> Proposal:
        return Proposal(
            conf_change=cc.clone(),
        )

    @staticmethod
    def normal(key: int, value: str) -> Proposal:
        return Proposal(
            normal=(key, value),
        )


PeerStubs = Dict[int, node_grpc.FrangoNodeStub]


class NodeConsensus:
    def __init__(self, raft_group, peer_stubs: PeerStubs, config: RaftConfig) -> None:
        self.raft_group: rraft.InMemoryRawNode = raft_group
        self.config: RaftConfig = config

        # TODO: use database
        self.kv_pairs: Dict[int, str] = dict()

        # used to send grpc calls
        self._peer_stubs: PeerStubs = peer_stubs

        # pushed by proposal() thread, popped by _handle_committed_entries, which also sends the success signal
        self._proposals: Queue[Proposal] = Queue()

        # pushed from outer environment, consumed by inner threads
        self._stop_chan: Queue[None] = Queue(maxsize=1)

        # signaled when raft might be ready, consumed in _wait_ready_loop()
        self._on_ready_cond: asyncio.Condition = asyncio.Condition()

        # signaled when becoming a leader, consumed in propose()
        # TODO: redirect the client to leader thread
        self._become_leader_cond: asyncio.Condition = asyncio.Condition()

        # ensure that the raft state modification is atomic
        self._raft_state_lock: asyncio.Lock = asyncio.Lock()

    def send_stop_signal(self) -> None:
        if not self._stop_chan.full():
            self._stop_chan.put_nowait(None)

    async def _send_messages(self, msgs: List[rraft.Message]) -> None:
        for msg in msgs:
            get_to = msg.get_to()
            try:
                if msg.get_msg_type() in (rraft.MessageType.MsgHeartbeat, rraft.MessageType.MsgHeartbeatResponse):
                    logger.trace(f"send heartbeat: {msg}")
                else:
                    logger.debug(f"sending msg to {get_to}: {msg}")
                peer_stub = self._peer_stubs[get_to]
                await peer_stub.RRaft(node_pb.RRaftMessage(bytes=msg.encode()))
            except grpc.AioRpcError as e:
                if e.code() == grpcStatus.UNAVAILABLE:
                    logger.warning(f"peer {get_to} not available")
                else:
                    raise e

    async def _handle_committed_entries(self, committed_entries: List[rraft.Entry], store: rraft.MemStorage) -> None:
        for entry in committed_entries:
            logger.success(f'handle committed (index={entry.get_index()}, type={entry.get_entry_type()}, '
                           f'data={str(entry.get_data())})')
            if not entry.get_data():
                # From new elected leaders.
                continue

            if entry.get_entry_type() == rraft.EntryType.EntryNormal:
                # For normal proposals, extract the key-value pair and then
                # insert them into the kv engine.
                data = str(entry.get_data(), "utf-8")
                reg = re.compile(r"put ([0-9]+) (.+)")

                if caps := reg.match(data):
                    key, value = int(caps.group(1)), str(caps.group(2))
                    self.kv_pairs[key] = value

            elif entry.get_entry_type() == rraft.EntryType.EntryConfChange:
                cc = rraft.ConfChange.default()
                new_conf = rraft.ConfChange.decode(entry.get_data())

                cc.set_id(new_conf.get_id())
                cc.set_node_id(new_conf.get_node_id())
                cc.set_context(new_conf.get_context())
                cc.set_change_type(new_conf.get_change_type())

                cs = self.raft_group.apply_conf_change(cc)
                store.wl().set_conf_state(cs)

            # notify that the proposal succeeded
            if self.raft_group.get_raft().get_state() == rraft.StateRole.Leader:
                proposal = self._proposals.get_nowait()
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
            # wait until ready
            async with self._on_ready_cond:
                await self._on_ready_cond.wait_for(
                    lambda: self.raft_group and self.raft_group.has_ready()
                )

            await self._on_ready()

    async def _on_ready(self) -> None:
        async with self._raft_state_lock:
            store: rraft.MemStorage = self.raft_group.get_raft().get_raft_log().get_store().clone()
            ready: rraft.Ready = self.raft_group.ready()

            if ready.messages():
                await self._send_messages(ready.take_messages())

            # Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
            snapshot_default: rraft.Snapshot = rraft.Snapshot.default()
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

            light_rd: rraft.LightReady = self.raft_group.advance(ready.make_ref())
            # Update commit index.
            if commit := light_rd.commit_index():
                store.wl().hard_state().set_commit(commit)

            await self._send_messages(light_rd.take_messages())
            await self._handle_committed_entries(light_rd.take_committed_entries(), store)
            self.raft_group.advance_apply()

    def is_leader(self):
        return self.raft_group.get_raft().get_state() == rraft.StateRole.Leader

    async def propose(self, proposal: Proposal) -> None:
        async with self._become_leader_cond:
            await self._become_leader_cond.wait_for(lambda: self.is_leader())
        async with self._raft_state_lock:
            self._proposals.put_nowait(proposal)
            self._raft_propose(proposal)
        await proposal.wait_until_success()

    def on_receive_msg(self, msg: rraft.Message, event_loop: asyncio.AbstractEventLoop) -> None:
        if msg.get_msg_type() in (rraft.MessageType.MsgHeartbeat, rraft.MessageType.MsgHeartbeatResponse):
            logger.trace(f"recv heartbeat: {msg}")
        else:
            logger.debug(f"recv msg from {msg.get_from()}: {msg}")

        async def step():
            async with self._raft_state_lock:
                self.raft_group.step(msg)
            if self.raft_group.has_ready():
                await self._notify_ready()

        event_loop.create_task(step())

    async def loop_until_stopped(self) -> None:
        wait_ready_loop = asyncio.create_task(self._wait_ready_loop())

        next_tick = asyncio.get_running_loop().time() + self.config.tick_seconds  # tick immediately
        while not self._stop_chan.full():

            # wait until next tick
            await asyncio.sleep(next_tick - asyncio.get_running_loop().time())
            next_tick = asyncio.get_running_loop().time() + self.config.tick_seconds

            # do the tick
            async with self._raft_state_lock:
                self.raft_group.tick()

            # notify the propose() thread
            async with self._become_leader_cond:
                if self.is_leader():
                    self._become_leader_cond.notify_all()

            # handle readies
            if self.raft_group.has_ready():
                await self._notify_ready()

        await wait_ready_loop

    def leader_id(self) -> Optional[int]:
        leader_id = self.raft_group.get_raft().get_leader_id()
        if leader_id == rraft.INVALID_ID:
            return None
        else:
            return leader_id
