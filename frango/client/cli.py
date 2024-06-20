import json
from argparse import ArgumentParser
import time

import grpc
from loguru import logger

from rich.console import Console
from rich.table import Table

from frango.pb import node_pb, node_grpc
from frango.config import DEFAULT_CONFIG_PATH, get_config


def ping(stub: node_grpc.FrangoNodeStub) -> None:
    start = time.time()
    ping_resp: node_pb.PingResp = stub.Ping(node_pb.Empty())
    ms = (time.time() - start) * 1000
    logger.info(f'Ping ({ms:.2f} ms): id={ping_resp.id}, leader_id={ping_resp.leader_id}')


def query(stub: node_grpc.FrangoNodeStub, query_str: str) -> None:
    start = time.time()
    query_req = node_pb.QueryReq(query_str=query_str)
    query_resp: node_pb.QueryResp = stub.Query(query_req)
    ms = (time.time() - start) * 1000

    logger.info(f'query "{query_str}" returns afters ({ms:.2f} ms) is_valid={query_resp.is_valid}')

    if query_resp.is_error:
        logger.error(f'query "{query_str}" returned error:\n{query_resp.err_msg}')
    elif query_resp.is_valid:
        table = Table(title="Query result")
        for col_name in query_resp.header:
            table.add_column(col_name)

        for row_json in query_resp.rows_in_json:
            row = json.loads(row_json)
            assert isinstance(row,  list)
            assert len(row) == len(query_resp.header), f'row length {len(row)} != {len(query_resp.header)}'
            table.add_row(*row)

        logger.info(f'{table}')
        console = Console()
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

    args = parser.parse_args()

    config = get_config(args.config)
    peers_dict = {peer.node_id: peer for peer in config.peers}
    listen = peers_dict[args.i].listen if args.i else config.peers[0].listen
    stub = node_grpc.FrangoNodeStub(channel=grpc.insecure_channel(listen))  # type: ignore[no-untyped-call]

    if args.command == 'ping':
        ping(stub)
    elif args.command == 'query':
        query(stub, args.query_arg)
    else:
        logger.error(f'Unknown command {args.command}')
        parser.print_help()


if __name__ == '__main__':
    main()
