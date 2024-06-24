#!/usr/bin/env python3

import argparse
import ipaddress
import multiprocessing
import pathlib
import sys

import pgcrawl.consts as PG_CONSTS
import pgcrawl.dispatch as PG_DISPATCH
from pgcrawl.logging import log


PARSER = argparse.ArgumentParser(
    prog="pagegraph-tranco-crawl dispatcher",
    description="Script responsible for dispatching and coordinating calls "
                "to child SSH server.\n"
                "The full set of steps to take to set up a client from "
                "scratch are: --test-connection, --install-client-code, "
                "--check-client-code, --setup-client-code",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
PARSER.add_argument(
    "ip",
    help="The IP address of the client server to interact with.")
PARSER.add_argument(
    "--test-connection",
    default=False,
    action="store_true",
    help="Test connection to child server, but don't make any changes.")
PARSER.add_argument(
    "--delete-client-code",
    default=False,
    action="store_true",
    help="Install code at `--client-code-path` if present.")
PARSER.add_argument(
    "--install-client-code",
    default=False,
    action="store_true",
    help="Install this crawling code on child server.")
PARSER.add_argument(
    "--check-client-code",
    default=False,
    action="store_true",
    help="Check this crawling code is installed on child server, then quit.")
PARSER.add_argument(
    "--setup-client-code",
    default=False,
    action="store_true",
    help="Run the set up script for this code on the child server.")
PARSER.add_argument(
    "--client-code-path",
    default=PG_CONSTS.DEFAULT_CLIENT_CODE_PATH,
    help="Path to where this code should be installed on the client.")
PARSER.add_argument(
    "-u", "--user",
    default="ubuntu",
    help="The user to use when SSH'ing to a client server.")
PARSER.add_argument(
    "--quiet", "-q",
    default=False,
    action="store_true",
    help="Suppress non-error messages and logging.")

ARGS = PARSER.parse_args()
QUIET = ARGS.quiet
USER = ARGS.user
CLIENT_PATH = ARGS.client_code_path

IP = ipaddress.ip_address(ARGS.ip)

if ARGS.test_connection:
    log(f"Checking connection to {IP}.")
    if not PG_DISPATCH.test_connection(USER, IP, QUIET):
        sys.exit(1)
    sys.exit(0)

if ARGS.delete_client_code:
    log(f"Attempting to delete client code from {IP}:{CLIENT_PATH}.")
    if not PG_DISPATCH.delete_client_code(USER, IP, CLIENT_PATH, QUIET):
        sys.exit(1)
    sys.exit(0)

if ARGS.check_client_code:
    log(f"Checking if client code is installed on {IP}:{CLIENT_PATH}.")
    if not PG_DISPATCH.check_client_code(USER, IP, CLIENT_PATH, QUIET):
        sys.exit(1)
    sys.exit(0)

if ARGS.install_client_code:
    log(f"Attempting to install client code on {IP}:{CLIENT_PATH}.")
    if not PG_DISPATCH.install_client_code(USER, IP, CLIENT_PATH, QUIET):
        sys.exit(1)
    sys.exit(0)

if ARGS.setup_client_code:
    log(f"Attempting to run setup / init code on {IP}:{CLIENT_PATH}.")
    if not PG_DISPATCH.setup_client_code(USER, IP, CLIENT_PATH, QUIET):
        sys.exit(1)
    sys.exit(0)


# def name_to_parts(name):
#   index = name.index("_")
#   rank = name[0:index]
#   domain = name[index + 1:]
#   return rank, domain


# def run_cmd(info):
#   ip, command = info
#   Connection(host=ip, user="ubuntu").run(command)


# if __name__ == '__main__':
#   instance_ips = [
#   ]

#   todo_path_contents = pathlib.Path("./todo/").iterdir()
#   todo_files = list(filter(lambda x: x.is_file(), todo_path_contents))

#   chunk_size = len(instance_ips)
#   num_todo = len(todo_files)
#   index = 0
#   total = 0
#   while index < num_todo:
#     workload = itertools.islice(todo_files, index, index + chunk_size)
#     parsed_workload = [name_to_parts(x.name) for x in workload]
#     print(parsed_workload)
#     commands = [f"python3 /home/ubuntu/crawl.py {domain} {rank}"
#         for rank, domain in parsed_workload]
#     ip_cmds = zip(instance_ips, commands)
#     with multiprocessing.Pool(chunk_size) as p:
#       p.map(run_cmd, ip_cmds)
#     total += len(list(workload))
#     index += chunk_size
#     break
#   print(total)
