from dataclasses import dataclass


@dataclass
class PeerConfig:
    peer_id: int
    listen: str


TICK_SECONDS = 0.1
