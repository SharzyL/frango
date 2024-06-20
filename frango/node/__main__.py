import asyncio
from argparse import ArgumentParser
from pathlib import Path
from loguru import logger
import sys

from frango.sql_adaptor import SQLDef
from frango.table_def import User, Article, Read
from frango.node.node import FrangoNode
from frango.config import get_config_default


async def async_main() -> None:
    parser = ArgumentParser(description='Frango node')
    parser.add_argument('--debug', action='store_true', help='enable debug mode')
    parser.add_argument('--create', action='store_true', help='create table')
    parser.add_argument('--bulk-load', type=Path, default=None, help='load db from basedir')
    parser.add_argument('-i', type=int, required=True, help='index of node')
    parser.add_argument('-c', '--config', type=Path, default="./etc/default.toml", help='config file')

    args = parser.parse_args()

    log_level = "DEBUG" if args.debug else "INFO"
    logger.remove()
    logger.add(sys.stdout, colorize=True, level=log_level)

    config = get_config_default(args.config)
    known_classes = {
        "Article": Article,
        "User": User,
        "Read": Read,
    }
    frango_node = FrangoNode(args.i, config, known_classes=known_classes)

    if args.create:
        for cls in (Article, User, Read):
            assert issubclass(cls, SQLDef)
            frango_node.storage.execute(cls.sql_drop_if_exists())
            frango_node.storage.execute(cls.sql_create())
        frango_node.storage.commit()

    if args.bulk_load is not None:
        table_dat_files = {
            "Article": args.bulk_load / "article.dat",
            "User": args.bulk_load / "user.dat",
            "Read": args.bulk_load / "read.dat",
        }
        frango_node.bulk_load(table_dat_files)
        frango_node.storage.commit()

    await frango_node.loop()


def main() -> None:
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
