from argparse import ArgumentParser
import time

import grpc
from loguru import logger

from frango.pb import node_pb, node_grpc
from frango.config import DEFAULT_CONFIG_PATH, get_config


def ping(stub: node_grpc.FrangoNodeStub) -> None:
    start = time.time()
    ping_resp: node_pb.PingResp = stub.Ping(node_pb.Empty())
    ms = (time.time() - start) * 1000
    logger.info(f'Ping ({ms:.2f} ms): id={ping_resp.id}, leader_id={ping_resp.leader_id}')


def query(stub: node_grpc.FrangoNodeStub, query_str: str) -> None:
    start = time.time()
    ping_resp: node_pb.PingResp = stub.Ping(node_pb.Empty())
    ms = (time.time() - start) * 1000
    logger.info(f'Ping ({ms:.2f} ms): id={ping_resp.id}, leader_id={ping_resp.leader_id}')


def main() -> None:
    parser = ArgumentParser(description='Frango API client')
    parser.add_argument('-c', '--config', type=str, help='configuration file path',
                        default=DEFAULT_CONFIG_PATH)
    parser.add_argument('-i', type=int, help='default peer id to query')

    subparsers = parser.add_subparsers(dest='command')

    # ping command
    ping_parser = subparsers.add_parser('ping', help='Ping command')

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
        raise NotImplementedError
    else:
        logger.error(f'Unknown command {args.command}')
        parser.print_help()


if __name__ == '__main__':
    main()
