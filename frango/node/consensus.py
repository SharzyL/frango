from __future__ import annotations
import asyncio
from asyncio import Queue
import re
import time
from collections import deque
from typing import Dict, List, Optional, Tuple, Deque

import rraft
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
    StateRole, ConfChangeType, OverflowStrategy,
)
from loguru import logger

from frango.pb import node_pb, node_grpc


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


def example_config() -> Config:
    cfg = Config.default()
    cfg.set_election_tick(10)
    cfg.set_heartbeat_tick(3)
    return cfg


def is_initial_msg(msg: Message) -> bool:
    """
    The message can be used to initialize a raft node or not.
    """
    msg_type = msg.get_msg_type()
    return (
            msg_type == MessageType.MsgRequestVote
            or msg_type == MessageType.MsgRequestPreVote
            or (msg_type == MessageType.MsgHeartbeat and msg.get_commit() == 0)
    )


PeerStubs = Dict[int, node_grpc.FrangoNodeStub]


class NodeConsensus:
    def __init__(self, raft_group: Optional[InMemoryRawNode], peer_stubs: PeerStubs) -> None:
        self.raft_group = raft_group
        self.kv_pairs = dict()

        self._peer_stubs = peer_stubs
        self._proposals: Deque[Proposal] = deque()
        self._mailbox: Queue[Message] = Queue()
        self._stop_chan: Queue[None] = Queue(maxsize=1)

    def send_stop_signal(self):
        if not self._stop_chan.full():
            self._stop_chan.put_nowait(None)

    @staticmethod
    def create_raft_leader(self_peer_id: int, peer_stubs: PeerStubs) -> "NodeConsensus":
        """
        Create a raft leader only with itself in its configuration.
        """
        cfg = example_config()
        cfg.set_id(self_peer_id)
        s = Snapshot.default()
        # Because we don't use the same configuration to initialize every node, so we use
        # a non-zero index to force new followers catch up logs by snapshot first, which will
        # bring all nodes to the same initial state.
        s.get_metadata().set_index(1)
        s.get_metadata().set_term(1)
        s.get_metadata().get_conf_state().set_voters([1])
        storage = MemStorage()
        storage.wl().apply_snapshot(s)
        raft_group = InMemoryRawNode(cfg, storage, Logger(chan_size=4096, overflow_strategy=OverflowStrategy.Block))
        return NodeConsensus(raft_group, peer_stubs)

    @staticmethod
    def create_raft_follower(peer_stubs: PeerStubs) -> "NodeConsensus":
        """
        Create a raft follower.
        """
        return NodeConsensus(None, peer_stubs)

    def maybe_init_with_message(self, msg: Message) -> None:
        """
        Step a raft message, initialize the raft if needed.
        """
        if self.raft_group is None:
            if is_initial_msg(msg):
                logger.info("init")
                cfg = example_config()
                cfg.set_id(msg.get_to())
                storage = MemStorage()
                self.raft_group = InMemoryRawNode(cfg, storage,
                                                  Logger(chan_size=4096, overflow_strategy=OverflowStrategy.Block))
            else:
                return

        self.raft_group.step(msg)

    def receive_message(self, msg: Message) -> None:
        # logger.debug(f"recv msg: {msg}")
        self._mailbox.put_nowait(msg)

    async def send_messages(self, msgs: List[Message]) -> None:
        for msg in msgs:
            get_to = msg.get_to()
            try:
                peer_stub = self._peer_stubs[get_to]
                await peer_stub.RRaft(node_pb.RRaftMessage(bytes=msg.encode()))

            except Exception as qe:
                logger.error(f"send raft message to {get_to} fail: “{qe}”")

    async def handle_committed_entries(self, committed_entries: List[Entry], store: MemStorage) -> None:
        for entry in committed_entries:
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

            if self.raft_group.get_raft().get_state() == StateRole.Leader:
                # TODO: The leader should respond to the clients, tell them if their proposals succeeded or not.
                proposal = self._proposals.popleft()
                proposal.send_success_signal(True)

    def append_proposal(self, proposal: Proposal) -> None:
        self._proposals.append(proposal)

    def propose(self, proposal: Proposal) -> None:
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

    async def on_ready(self) -> None:
        store = self.raft_group.get_raft().get_raft_log().get_store().clone()
        # Get the `Ready` with `RawNode::ready` interface.
        ready = self.raft_group.ready()

        if ready.messages():
            # Send out the messages come from the node.
            await self.send_messages(ready.take_messages())

        # Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        snapshot_default = Snapshot.default()
        if ready.snapshot() != snapshot_default.make_ref():
            s = ready.snapshot().clone()
            store.wl().apply_snapshot(s)

        # Apply all committed entries.
        await self.handle_committed_entries(ready.take_committed_entries(), store)

        # Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        # raft logs to the latest position.
        store.wl().append(ready.entries())

        if hs := ready.hs():
            # Raft HardState changed, and we need to persist it.
            store.wl().set_hardstate(hs)

        if persisted_msgs := ready.take_persisted_messages():
            await self.send_messages(persisted_msgs)

        # Call `RawNode::advance` interface to update position flags in the raft.
        light_rd = self.raft_group.advance(ready.make_ref())
        # Update commit index.
        if commit := light_rd.commit_index():
            store.wl().hard_state().set_commit(commit)

        # Send out the messages.
        await self.send_messages(light_rd.take_messages())
        # Apply all committed entries.
        await self.handle_committed_entries(light_rd.take_committed_entries(), store)
        # Advance the apply index.
        self.raft_group.advance_apply()

    def is_leader(self):
        return self.raft_group is not None and self.raft_group.get_raft().get_state() == StateRole.Leader

    async def add_peers(self):
        for peer_id, peer in self._peer_stubs.items():
            conf_change = ConfChange.default()
            conf_change.set_node_id(peer_id)
            conf_change.set_change_type(ConfChangeType.AddNode)
            self.append_proposal(Proposal(conf_change=conf_change))

    async def loop_until_stopped(self) -> None:
        last_tick = time.time()
        while True:

            # check mailbox every 10ms
            await asyncio.sleep(0.01)
            while not self._mailbox.empty():
                msg = self._mailbox.get_nowait()
                self.maybe_init_with_message(msg)
                if self.raft_group is not None:
                    self.raft_group.step(msg)

            # tick raft_group every 100ms
            if self.raft_group is not None and time.time() - last_tick > 0.1:
                self.raft_group.tick()
                last_tick = time.time()

            # Let the leader pick pending proposals from the global queue.
            if self.raft_group and self.raft_group.get_raft().get_state() == StateRole.Leader:
                # Handle new proposals.
                for p in self._proposals:
                    if p.proposed is None:
                        self.propose(p)

            # handle readies
            if self.raft_group and self.raft_group.has_ready():
                await self.on_ready()

            # check stop signal
            if self._stop_chan.full():
                break
