from dataclasses import dataclass, field
from dataclass_wizard import fromdict
from typing import List, Optional, cast
from pathlib import Path

import tomllib



@dataclass
class Config:
    @dataclass
    class Peer:
        node_id: int
        listen: str

    @dataclass
    class Raft:
        tick_seconds: float = field(default=0.1)

    @dataclass
    class Partition:
        type: str

        # valid if type == "regular"
        filter: dict[int, str] = field(default_factory=dict)

        # valid if type == "dependent"
        dependency_table: str = field(default="")
        dependency_key: str = field(default="")

    peers: List[Peer]
    db_path_pattern: str
    raft: Raft = field(default_factory=Raft)
    partitions: dict[str, Partition] = field(default_factory=dict)


DEFAULT_CONFIG_PATH = "./etc/frango.toml"


def get_config(path: str | Path) -> Config:
    with open(path, "rb") as f:
        data = tomllib.load(f)
        return cast(Config, fromdict(Config, data))


def get_config_default(path: Optional[str | Path]) -> Config:
    return get_config(DEFAULT_CONFIG_PATH if path is None else path)
