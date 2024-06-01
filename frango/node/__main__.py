import asyncio
from argparse import ArgumentParser
from pathlib import Path
from loguru import logger
import sys

from frango.data_model import User, Article, Read
from frango.node.node import PeerConfig, FrangoNode


def load_data_from_basedir(base_dir: Path):
    users = []
    with open(base_dir / 'user.dat', 'r') as f:
        for line in f.readlines():
            users.append(User.from_json(line))

    articles = []
    with open(base_dir / 'article.dat', 'r') as f:
        for line in f.readlines():
            articles.append(Article.from_json(line))
    print(articles[0])

    reads = []
    with open(base_dir / 'read.dat', 'r') as f:
        for line in f.readlines():
            reads.append(Read.from_json(line))


peers = [
    PeerConfig(peer_id=1, listen="127.0.0.1:45001"),
    PeerConfig(peer_id=2, listen="127.0.0.1:45002"),
]


async def async_main():
    parser = ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('-i', type=int, required=True)
    args = parser.parse_args()

    log_level = "DEBUG" if args.debug else "INFO"
    logger.remove()
    logger.add(sys.stdout, colorize=True, level=log_level)

    peers_dict = {peer.peer_id: peer for peer in peers}
    frango_node = FrangoNode(args.i, peers_dict)
    await frango_node.loop()


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
