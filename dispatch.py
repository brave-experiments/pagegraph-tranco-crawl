#!/usr/bin/env python3

import argparse
import multiprocessing
import pathlib
import sys

import pgcrawl.dispatch as PG_DISPATCH
from pgcrawl.logging import log


PARSER = argparse.ArgumentParser(
    prog="pagegraph-tranco-crawl dispatcher",
    description="Script responsible for dispatching and coordinating calls "
                "to child SSH servers.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
PARSER.add_argument(
  "-f", "--filepath",
  default="./ips.txt",
  help="Path to a file of IP addresses, describing the child servers to "
       "use to crawl pages.")
PARSER.add_argument(
  "--test-connections",
  default=False,
  action="store_true",
  help="Test connections to child servers, but don't make any changes.")
PARSER.add_argument(
  "--install-client-code",
  default=False,
  action="store_true",
  help="Install this crawling code on child servers, and then quit.")
PARSER.add_argument(
  "--client-code-path",
  default="~/pagegraph-tranco-crawl,
  help="Relative path to where this code should be installed on each client.")
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

FILEPATH = pathlib.Path(ARGS.filepath)

IP_ADDRESSES = PG_DISPATCH.read_ips(FILEPATH)
if ARGS.test_connections:
  log(f"Checking connections to {len(IP_ADDRESSES)} child servers")
  for ip in IP_ADDRESSES:
    if not PG_DISPATCH.test_connection(USER, ip, QUIET):
      sys.exit(1)
  sys.exit(0)

if ARGS.install_client:
  CLIENT_PATH = ARGS.client_code_path
  log(f"Installing on {len(IP_ADDRESSES)} servers, at {CLIENT_PATH}")



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
#     commands = [f"python3 /home/ubuntu/crawl.py {domain} {rank}" for rank, domain in parsed_workload]
#     ip_cmds = zip(instance_ips, commands)
#     with multiprocessing.Pool(chunk_size) as p:
#       p.map(run_cmd, ip_cmds)
#     total += len(list(workload))
#     index += chunk_size
#     break
#   print(total)