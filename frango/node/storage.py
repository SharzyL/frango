import sqlite3
from pathlib import Path

from loguru import logger
import sqlglot.expressions as exp

from frango.node.sql_schedule import sql_to_str


class StorageBackend:
    def __init__(self, db_path: Path):
        logger.info(f'sql connect to "{db_path}"')
        self.db_conn = sqlite3.connect(db_path)
        self.db_conn.autocommit = False

    def execute(self, query: str | exp.Expression | list[exp.Expression]):
        cursor = self.db_conn.cursor()

        def execute_one(stmt_: exp.Expression):
            stmt_str = sql_to_str(stmt_)
            if '_params' in stmt_.args:
                params = stmt_.args.get('_params')
                if isinstance(params, dict):
                    logger.info(f'sql execute: `{stmt_str}` with params: {params}')
                    cursor.execute(stmt_str, params)
                elif hasattr(params, '__iter__'):
                    logger.info(f'sql execute: `{stmt_str}` with {len(params)} params')
                    cursor.executemany(stmt_str, params)
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

        # TODO: read out

    def commit(self):
        logger.info(f'sql commit')
        self.db_conn.commit()

    def rollback(self):
        logger.info(f'sql rollback')
        self.db_conn.rollback()
