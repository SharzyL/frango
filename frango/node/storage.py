from __future__ import annotations

import threading
from dataclasses import dataclass, field
import sqlite3
from pathlib import Path
import json
from typing import Optional, List, Sequence, Tuple, Callable, Any

from loguru import logger
import sqlglot.expressions as exp

from frango.node.sql_schedule import sql_to_str, SQLVal
from frango.pb import node_pb


@dataclass
class QueryResult:
    err_msg: Optional[str] = field(default=None)
    rows: List[Sequence[SQLVal]] = field(default_factory=list)
    header: List[str] = field(default_factory=list)
    is_valid: bool = field(default=False)
    is_error: bool = field(default=False)

    def to_pb(self) -> node_pb.QueryResp:
        assert not (self.is_error and self.is_valid)  # we cannot have both error and header
        return node_pb.QueryResp(err_msg=None, is_valid=self.is_valid, is_error=self.is_error,
                                 header=self.header, rows_in_json=map(json.dumps, self.rows))

    @staticmethod
    def from_pb(resp: node_pb.QueryResp) -> QueryResult:
        assert not (resp.is_error and resp.header)
        return QueryResult(err_msg=resp.err_msg, is_valid=resp.is_valid, is_error=False,
                           header=list(resp.header), rows=list(map(json.loads, resp.rows_in_json)))

    def merge(self, other: QueryResult) -> None:
        if other.is_valid:
            assert self.is_valid
            # sqlite
            if len(self.header) == 0 and len(other.header) > 0:
                self.header = other.header
            self.rows.extend(other.rows)
            self.header = other.header
        elif other.err_msg is not None:
            self.is_valid = False
            if self.err_msg is None:
                self.err_msg = ""
            self.err_msg += f"\n{other.err_msg}"


class StorageBackend:
    def __init__(self, db_path: Path):
        logger.info(f'sql connect to "{db_path}"')
        self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
        self.db_conn.autocommit = False
        self.mutex = threading.Lock()

    def execute(self, query: str | exp.Expression | List[exp.Expression]) -> QueryResult:
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
                logger.info(f'sql execute `{query}`')
                cursor.execute(stmt_str)

        try:
            if isinstance(query, str):
                logger.info(f'sql execute `{query}`')
                cursor.execute(query)
            elif isinstance(query, exp.Expression):
                execute_one(query)
            elif isinstance(query, list):
                for stmt in query:
                    execute_one(stmt)

            rows: List[Sequence[SQLVal]] = cursor.fetchall()
            header = [i[0] for i in cursor.description] if cursor.description is not None else []
            is_valid = cursor.description is not None
            logger.info(f'description: {cursor.description}, header: {header}')
            return QueryResult(err_msg=None, rows=rows, header=header, is_valid=is_valid)

        except sqlite3.Error as e:
            logger.error(f'sql execute `{query}` error: {e}')
            return QueryResult(err_msg=repr(e), is_error=True)

    def commit(self) -> None:
        logger.info(f'sql commit')
        self.db_conn.commit()

    def rollback(self) -> None:
        logger.info(f'sql rollback')
        self.db_conn.rollback()
