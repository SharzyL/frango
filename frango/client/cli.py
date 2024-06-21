import json
from argparse import ArgumentParser
import time

import grpc
from loguru import logger

from rich.console import Console
from rich.table import Table
from rich.text import Text

from frango.pb import node_pb, node_grpc
from frango.config import DEFAULT_CONFIG_PATH, get_config


def ping(stub: node_grpc.FrangoNodeStub) -> None:
    start = time.time()
    ping_resp: node_pb.PingResp = stub.Ping(node_pb.Empty())
    ms = (time.time() - start) * 1000
    logger.info(f'Ping ({ms:.2f} ms): id={ping_resp.id}, leader_id={ping_resp.leader_id}')


def query(stub: node_grpc.FrangoNodeStub, query_str: str, max_display_rows: int) -> None:
    start = time.time()
    query_req = node_pb.QueryReq(query_str=query_str)
    query_resp: node_pb.QueryResp = stub.Query(query_req)
    ms = (time.time() - start) * 1000

    console = Console()

    if query_resp.is_error:
        console.log(f'query "{query_str}" returned error:\n{Text.from_ansi(query_resp.err_msg)}')
    elif not query_resp.is_valid:
        console.log(f'query "{query_str}" succeeded without error (takes {ms:.2f} ms)')

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


def main() -> None:
    parser = ArgumentParser(description='Frango API client')
    parser.add_argument('-c', '--config', type=str, help='configuration file path',
                        default=DEFAULT_CONFIG_PATH)
    parser.add_argument('-i', type=int, default=1, help='default peer id to query')

    subparsers = parser.add_subparsers(dest='command')

    # ping command
    _ = subparsers.add_parser('ping', help='Ping command')

    # query command
    query_parser = subparsers.add_parser('query', help='Query command')
    query_parser.add_argument('query_arg', type=str, help='Query argument')
    query_parser.add_argument('--max-rows', type=int, default=50,
                              help='Max rows to display on console, set to negative to disable')

    args = parser.parse_args()

    config = get_config(args.config)
    peers_dict = {peer.node_id: peer for peer in config.peers}
    listen = peers_dict[args.i].listen if args.i else config.peers[0].listen
    stub = node_grpc.FrangoNodeStub(channel=grpc.insecure_channel(listen))  # type: ignore[no-untyped-call]

    if args.command == 'ping':
        ping(stub)
    elif args.command == 'query':
        query(stub, args.query_arg, args.max_rows)
    else:
        logger.error(f'Unknown command {args.command}')
        parser.print_help()


if __name__ == '__main__':
    main()
