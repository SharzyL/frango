from __future__ import annotations

from dataclasses import dataclass, field
import sqlite3
from pathlib import Path
import json
from typing import Optional, List, Sequence, Tuple

from loguru import logger
import sqlglot.expressions as exp

from frango.node.sql_schedule import sql_to_str, SQLVal
from frango.pb import node_pb


@dataclass
class QueryResult:
    rows: List[Sequence[SQLVal]] = field(default_factory=list)
    header: List[str] = field(default_factory=list)

    def to_pb(self) -> node_pb.QueryResp:
        resp = node_pb.QueryResp(error=1, header=self.header, rows_in_json=map(json.dumps, self.rows))
        return resp

    @staticmethod
    def from_pb(resp: node_pb.QueryResp) -> QueryResult:
        return QueryResult(header=list(resp.header), rows=list(map(json.loads, resp.rows_in_json)))

    @staticmethod
    def merge(self: Optional[QueryResult], other: QueryResult) -> QueryResult:
        if self is None:
            return other
        else:
            self.rows.extend(other.rows)
            return self

    def is_valid(self) -> bool:
        return len(self.header) > 0


class StorageBackend:
    def __init__(self, db_path: Path):
        logger.info(f'sql connect to "{db_path}"')
        self.db_conn = sqlite3.connect(db_path)
        self.db_conn.autocommit = False

    def execute(self, query: str | exp.Expression | list[exp.Expression]) -> QueryResult:
        cursor = self.db_conn.cursor()

        def execute_one(stmt_: exp.Expression) -> None:
            stmt_str = sql_to_str(stmt_)
            # handle parametrized query
            if '_params' in stmt_.args:
                params = stmt_.args.get('_params')
                if isinstance(params, dict):
                    logger.info(f'sql execute: `{stmt_str}` with params: {params}')
                    cursor.execute(stmt_str, params)
                elif isinstance(params, list):
                    logger.info(f'sql execute: `{stmt_str}` with {len(params)} params')
                    cursor.executemany(stmt_str, params)
                else:
                    raise ValueError(f'sql execute: `{stmt_str}` with unknown params type: params={params}')
            else:
                cursor.execute(stmt_str)

        if isinstance(query, str):
            logger.info(f'sql execute `{query}`')
            cursor.execute(query)
        elif isinstance(query, exp.Expression):
            execute_one(query)
        elif isinstance(query, list):
            for stmt in query:
                execute_one(stmt)

        rows: List[Sequence[SQLVal]] = cursor.fetchall()
        header = [i[0] for i in cursor.description]
        return QueryResult(rows, header)

    def commit(self) -> None:
        logger.info(f'sql commit')
        self.db_conn.commit()

    def rollback(self) -> None:
        logger.info(f'sql rollback')
        self.db_conn.rollback()
