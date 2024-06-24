import json
from argparse import ArgumentParser
import time
from pathlib import Path
from typing import List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor

import grpc
import rich
from loguru import logger

from rich.console import Console
from rich.table import Table
from rich.text import Text

from frango.pb import node_pb, node_grpc
from frango.config import DEFAULT_CONFIG_PATH, get_config
from frango.sql_adaptor import sql_parse


def ping(stub: node_grpc.FrangoNodeStub) -> None:
    start = time.time()
    ping_resp: node_pb.PingResp = stub.Ping(node_pb.Empty())
    ms = (time.time() - start) * 1000
    logger.info(f'Ping ({ms:.2f} ms): id={ping_resp.id}, leader_id={ping_resp.leader_id}')


def print_result(query_resp: node_pb.QueryResp, ms: float, max_display_rows: int = -1) -> None:
    console = Console()

    if query_resp.is_error:
        console.log(f'query returned error:\n{Text.from_ansi(query_resp.err_msg)}')
    elif not query_resp.is_valid:
        console.log(f'query succeeded without error (takes {ms:.2f} ms)')

    elif query_resp.is_valid:
        rows = list(map(json.loads, query_resp.rows_in_json))
        for i, row in enumerate(rows):
            assert isinstance(row, list)
            assert len(row) == len(query_resp.header), f'row length {len(row)} != {len(query_resp.header)} in row {i}'

        table = Table(title=f'Query result ({len(rows)} rows, takes {ms:.2f} ms)')
        for col_name in query_resp.header:
            table.add_column(col_name)

        if len(rows) > 0:
            row0 = rows[0]
            for i, col in enumerate(row0):
                if isinstance(col, int):
                    table.columns[i].style = 'cyan'
                elif isinstance(col, str):
                    table.columns[i].style = 'yellow'
                elif isinstance(col, float):
                    table.columns[i].style = 'magenta'

        for i, row_json in enumerate(query_resp.rows_in_json):
            row = json.loads(row_json)
            assert isinstance(row, list)
            assert len(row) == len(query_resp.header), f'row length {len(row)} != {len(query_resp.header)}'
            # print i = 0, 1, ..., max_display_rows - 3
            # then print a row of ellipses
            # then print the final two rows
            if i == max_display_rows:
                table.add_row(*(["..."] * len(query_resp.header)))
            elif 0 <= max_display_rows < i < len(query_resp.rows_in_json) - 2:
                pass
            else:
                table.add_row(*map(str, row))

        console.print(table)


def query(stub: node_grpc.FrangoNodeStub, query_str: str, max_display_rows: int, is_local: bool) -> None:
    start = time.time()
    query_req = node_pb.QueryReq(query_str=query_str)
    query_resp: node_pb.QueryResp = stub.LocalQuery(query_req) if is_local else stub.Query(query_req)
    ms = (time.time() - start) * 1000
    print_result(query_resp, ms, max_display_rows)


def popular_rank(stub: node_grpc.FrangoNodeStub, day: str,
                 temporal_granularity: node_pb.PopularRankReq.TemporalGranularity) -> None:
    start = time.time()
    query_req = node_pb.PopularRankReq(day=day, temporal_granularity=temporal_granularity)
    query_resp: node_pb.QueryResp = stub.PopularRank(query_req)
    ms = (time.time() - start) * 1000
    print_result(query_resp, ms)


def status(stubs: Dict[int, node_grpc.FrangoNodeStub]) -> None:
    def do_ping(stub_: node_grpc.FrangoNodeStub) -> Tuple[float, node_pb.PingResp]:
        start = time.time()
        resp_ = stub_.Ping(node_pb.Empty())
        ms_ = (time.time() - start) * 1000
        return ms_, resp_

    for node_id, stub in stubs.items():
        ms, resp = do_ping(stub)
        rich.print(f'Node {node_id}')
        rich.print(f'  Latency: ({ms:.2f} ms)')
        rich.print(f'  Database size: {resp.db_size_bytes / 1024. / 1024.:.2f} MB')
        rich.print(f'  Database location: {resp.db_location}')
        rich.print(f'  Last minute requests: {resp.requests_last_minute}')


def main() -> None:
    parser = ArgumentParser(description='Frango API client')
    parser.add_argument('-c', '--config', type=str, help='configuration file path',
                        default=DEFAULT_CONFIG_PATH)

    subparsers = parser.add_subparsers(dest='command')

    # ping command
    _ = subparsers.add_parser('ping', help='Ping command')

    # query command
    query_parser = subparsers.add_parser('query', help='execute SQL query')
    query_parser.add_argument('query_arg', type=str, help='query argument', nargs='?')
    query_parser.add_argument('-f', '--file', type=Path, help='path to sql file', default=None)
    query_parser.add_argument('--local', action='store_true', help='use LocalQuery to force local query')
    query_parser.add_argument('--max-rows', type=int, default=50,
                              help='max rows to display on console, set to negative to disable')
    query_parser.add_argument('-i', type=int, default=1, help='default peer id to query')

    # parse command
    parse_parser = subparsers.add_parser('parse', help='display the AST of parsed SQL for debugging purpose')
    parse_parser.add_argument('query_arg', type=str, help='sql string', nargs='?')
    parse_parser.add_argument('-f', '--file', type=Path, help='path to sql file', default=None)
    parse_parser.add_argument('-i', type=int, default=1, help='default peer id to query')

    # popularRank command
    rank_parser = subparsers.add_parser('popular-rank', help='query the popular rank')
    rank_parser.add_argument('day', type=str, help='begin of time range in iso format')
    rank_parser.add_argument('-g', type=str, help='temporal granularity', default='daily')
    rank_parser.add_argument('-i', type=int, default=1, help='default peer id to query')

    _ = subparsers.add_parser('status', help='monitor server status')

    args = parser.parse_args()

    config = get_config(args.config)
    peers_dict = {peer.node_id: peer for peer in config.peers}
    stubs = {
        node_id: node_grpc.FrangoNodeStub(
            channel=grpc.insecure_channel(peers_dict[node_id].listen)
        )  # type: ignore[no-untyped-call]
        for node_id in peers_dict.keys()
    }

    if args.command == 'ping':
        ping(stubs[args.i])
    elif args.command == 'query':
        query_str = args.query_arg
        is_local = args.local
        if args.file is not None:
            assert isinstance(args.file, Path)
            query_str = args.file.read_text()
        query(stubs[args.i], query_str, args.max_rows, is_local)
    elif args.command == 'parse':
        query_str = args.query_arg
        if args.file is not None:
            assert isinstance(args.file, Path)
            query_str = args.file.read_text()
        for stmt in sql_parse(query_str):
            rich.print(repr(stmt), end='\n\n')
    elif args.command == 'popular-rank':
        day = args.day
        granularity = node_pb.PopularRankReq.TemporalGranularity
        g = granularity.DAILY if args.g == 'daily' \
            else granularity.WEEKLY if args.g == 'weekly' \
            else granularity.MONTHLY if args.g == 'monthly' \
            else None
        if g is None:
            logger.error(f'Unknown temporal granularity: {args.g}')
            exit(1)
        popular_rank(stubs[args.i], day, g)
    elif args.command == 'status':
        status(stubs)
    else:
        logger.error(f'Unknown command {args.command}')
        parser.print_help()


if __name__ == '__main__':
    main()
