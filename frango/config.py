from dataclasses import dataclass, field
from dataclass_wizard import fromdict
from typing import List, Optional
from pathlib import Path

import tomllib


@dataclass
class Peer:
    peer_id: int
    listen: str


@dataclass
class Raft:
    tick_seconds: float = field(default=0.1)


@dataclass
class Config:
    peers: List[Peer]
    raft: Raft = field(default_factory=Raft)


DEFAULT_CONFIG_PATH = "./etc/default.toml"


def get_config(path: str | Path) -> Config:
    with open(path, "rb") as f:
        data = tomllib.load(f)
        return fromdict(Config, data)


def get_config_default(path: Optional[str | Path]) -> Config:
    return get_config(DEFAULT_CONFIG_PATH if path is None else path)
