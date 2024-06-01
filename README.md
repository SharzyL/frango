# Frangō: A Tiny Distributed Database

## Project Roadmap

Coordinator
- TOML configurable
- Interface to accept requests
- SQL parser and executor
- Distributed commit
- RPC to participants
- Manage participant migration and resize
- Monitor

Participant

- RPC from coordinator
- Operations
    - Bulk load
    - Query (read & filter)
    - Insert
    - Update
- (Optional) peer-to-peer data migration
- SQLite as the storage backend
