# Frango: A Distributed Database Engine Framework

## Features

- Raft-based consensus engine.
- Modular design that provides unified interface for different storage backend.
- Sophiscated SQL scheduler that supports a wide range of SQL commands.

## Usage

Frango is written in pure Python, built with package manager [PDM](https://pdm-project.org). It is configured with a TOML configuration file. The default configuration file path is `./etc/frango.toml`.

Frango provides two executable entries, `frango-node` and `frango-cli`. `frango-node` is the server node communicating with each other. `frango-cli` provides a client-side CLI interface that interacts with nodes.

### Environment Setup

Frango uses [Nix](https://nixos.org) as its development environemt manager and build manager. It also provides toolchains for non-Nix users. We provides the following approaches to deploy and develop Frango:

<details><summary>Nix development shell</summary>

You need to have Nix installed and `flake`, `nix-command` features enabled.

```console
$ nix develop
$ python frango/node -- [args..] # executes frango-node
$ python frango/client -- [args..] # executes frango-cli
```
</details>

<details><summary>Nix package</summary>

You need to have Nix installed and `flake`, `nix-command` features enabled.

```console
$ nix build
$ ./result/bin/frango-node [args..]
$ ./result/bin/frango-cli [args..]
```
</details>

<details><summary>Python source with dependencies managed by PDM.</summary>

You need to have PDM installed ([Official guide](https://pdm-project.org/latest/)) and Python no older than 3.12.

```console
$ pdm install
$ pdm node [args..]
$ pdm cli [args..]
```

If you do not want to install yet another package manager, you can try installing the dependencies shown in `pyproject.toml` manually, and run the corresponding source file with `PYTHONPATH=$PYTHONPATH:.`.
</details>

<details><summary>OCI-compatible container image (e.g. for Docker user).</summary>

Download the container image, e.g. `frango.tar.gz`.

```console
$ podman load -i frango.tar.gz
$ podman run --name frango -it --network host localhost/frango:latest /bin/frango-node [args..]
```

You can replace `podman` by your favorate container runtime, e.g. `docker`.
</details>

### Get started

To get started, suppose we have set up the environment. `../db-generation` stores the files `article.dat`, `user.dat`, `read.dat`, and `etc/frango.toml` is set correctly.

In the first shell, starts `frango-node -i 1 --bulk-load ../db-generation --create`. In the second shell, starts `frango-node -i 2 --bulk-load ../db-generation --create`. Note that `--bulk-load ../db-generation` and `--create` arguments are used to load the data, hence not required after the first run.

Now in the third shell, we can interact with nodes with `frango-cli`.

```
$ pdm cli query "SELECT datetime(timestamp / 1000, 'unixepoch') AS date, aid, readTimeLength FROM Read WHERE date BETWEEN '2017-09-24' AND '2017-09-26' LIMIT 20"
     Query result (20 rows, takes 6.12 ms)
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ date                ┃ aid  ┃ readTimeLength ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━━━━┩
│ 2017-09-25 09:38:17 │ 4844 │ 3              │
│ 2017-09-25 09:38:57 │ 2431 │ 74             │
│ 2017-09-25 09:39:07 │ 8071 │ 25             │
│ 2017-09-25 09:39:37 │ 2107 │ 61             │
│ 2017-09-25 09:39:57 │ 1668 │ 40             │
│ 2017-09-25 09:40:07 │ 6177 │ 62             │
│ 2017-09-25 09:40:47 │ 4188 │ 50             │
│ 2017-09-25 09:41:07 │ 8344 │ 70             │
│ 2017-09-25 09:41:37 │ 5333 │ 25             │
│ 2017-09-25 09:41:57 │ 896  │ 49             │
│ 2017-09-25 09:42:17 │ 2224 │ 65             │
│ 2017-09-25 09:42:27 │ 5139 │ 37             │
│ 2017-09-25 09:42:47 │ 8267 │ 52             │
│ 2017-09-25 09:42:57 │ 7950 │ 18             │
│ 2017-09-25 09:43:27 │ 1886 │ 84             │
│ 2017-09-25 09:43:57 │ 7079 │ 78             │
│ 2017-09-25 09:44:07 │ 5379 │ 95             │
│ 2017-09-25 09:44:57 │ 1115 │ 84             │
│ 2017-09-25 09:45:07 │ 6985 │ 19             │
│ 2017-09-25 09:45:17 │ 8834 │ 39             │
└─────────────────────┴──────┴────────────────┘
```

### Detail of Commands

The command `frango-node` starts a Frango node, with the following command-line interface:

```console
usage: -c [-h] [--debug] [--create] [--bulk-load BULK_LOAD] -i I [-c CONFIG]

Frango node

options:
  -h, --help            show this help message and exit
  --debug               enable debug mode
  --create              create pre-specified tables
  --bulk-load BULK_LOAD
                        load db from basedir
  -i I                  id of node
  -c CONFIG, --config CONFIG
                        config file
```

The command `frango-cli` interacts with Frango node in several ways.

```console
usage: -c [-h] [-c CONFIG] [-i I] {ping,query,parse,popular-rank} ...

Frango API client

positional arguments:
  {ping,query,parse,popular-rank}
    ping                Ping command
    query               execute SQL query
    parse               display the AST of parsed SQL for debugging purpose
    popular-rank        query the popular rank

options:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        configuration file path
  -i I                  default peer id to query
```
