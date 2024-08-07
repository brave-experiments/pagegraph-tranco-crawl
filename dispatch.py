#!/usr/bin/env python3

import argparse
import ipaddress
import sys

from pgcrawl import DEFAULT_CLIENT_CODE_PATH, NAME
from pgcrawl.client.args import ClientCrawlArgs, DEFAULT_CRAWL_ARGS
from pgcrawl.dispatch.commands import Action, client_setup, client_crawl
from pgcrawl.logging import add_logger_argument, Logger
from pgcrawl.types import IPAddress, UserName


def client_setup_cmd(args: argparse.Namespace, ips: list[IPAddress]) -> None:
    actions: list[Action] = []
    if args.test_connection:
        actions.append(Action.TEST_CONNECTION)
    if args.kill_child_processes:
        actions.append(Action.KILL_CHILD_PROCESSES)
    if args.delete_client_code:
        actions.append(Action.DELETE_CLIENT_CODE)
    if args.install_client_code:
        actions.append(Action.INSTALL_CLIENT_CODE)
    if args.check_client_code:
        actions.append(Action.CHECK_CLIENT_CODE)
    if args.setup_client_code:
        actions.append(Action.SETUP_CLIENT_CODE)
    if args.full_setup:
        actions.append(Action.ALL)
    return client_setup(actions, ips, args.user, args.client_code_path,
                        args.timeout, Logger(args.log_level))


def crawl_cmd(args: argparse.Namespace, ips: list[IPAddress]) -> None:
    client_crawl_args = ClientCrawlArgs(ARGS.client_code_path,
        ARGS.binary_path, ARGS.s3_bucket, ARGS.pagegraph_secs,
        ARGS.client_timeout)
    if args.limit != 0 and args.limit < len(ips):
        ips = ips[:args.limit]
    return client_crawl(ips, args.user, args.summarize, args.limit,
                        client_crawl_args, ARGS.timeout,
                        Logger(args.log_level))


PARSER = argparse.ArgumentParser(
    prog=f"{NAME}: dispatch",
    description="Script responsible for dispatching and coordinating calls "
                "to child SSH server.\n"
                "The full set of steps to take to set up a client from "
                "scratch are: --test-connection, --install-client-code, "
                "--check-client-code, --setup-client-code",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

SUBPARSERS = PARSER.add_subparsers(required=True)

CLIENT_SETUP_PARSER = SUBPARSERS.add_parser(
    "setup-clients",
    help="Validate, setup, configure, and otherwise prepare client servers.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
CLIENT_SETUP_PARSER.add_argument(
    "ip",
    help="The IP addresses of clients to interact with.",
    nargs="+")
CLIENT_SETUP_PARSER.add_argument(
    "--test-connection",
    default=False,
    action="store_true",
    help="Test connection to child server, but don't make any changes.")
CLIENT_SETUP_PARSER.add_argument(
    "--kill-child-processes",
    default=False,
    action="store_true",
    help="Kill relevant child processes on the child server.")
CLIENT_SETUP_PARSER.add_argument(
    "--delete-client-code",
    default=False,
    action="store_true",
    help="Install code at `--client-code-path` if present.")
CLIENT_SETUP_PARSER.add_argument(
    "--install-client-code",
    default=False,
    action="store_true",
    help="Install this crawling code on child server.")
CLIENT_SETUP_PARSER.add_argument(
    "--check-client-code",
    default=False,
    action="store_true",
    help="Check this crawling code is installed on child server, then quit.")
CLIENT_SETUP_PARSER.add_argument(
    "--setup-client-code",
    default=False,
    action="store_true",
    help="Run the set up script for this code on the child server.")
CLIENT_SETUP_PARSER.add_argument(
    "--full-setup",
    default=False,
    action="store_true",
    help="Run all commands needed to setup a client (i.e., --test-connection, "
         "--kill-child-processes, --delete-client-code, "
         "--install-client-code, --check-client-code, --setup-client-code).")
CLIENT_SETUP_PARSER.add_argument(
    "--client-code-path",
    default=DEFAULT_CLIENT_CODE_PATH,
    help="Path to where this code should be installed on the client.")
CLIENT_SETUP_PARSER.add_argument(
    "-u", "--user",
    default="ubuntu",
    type=UserName,
    help="The user to use when SSH'ing to a client server.")
CLIENT_SETUP_PARSER.add_argument(
    "-t", "--timeout",
    default=120,
    type=int,
    help="Maximum number of seconds to wait on a client to do anything.")
add_logger_argument(CLIENT_SETUP_PARSER)
CLIENT_SETUP_PARSER.set_defaults(func=client_setup_cmd)

CRAWL_PARSER = SUBPARSERS.add_parser(
    "crawl",
    help="Crawl URLs using child servers.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
CRAWL_PARSER.add_argument(
    "ip",
    help="The IP addresses of clients to interact with.",
    nargs="+")
CRAWL_PARSER.add_argument(
    "-u", "--user",
    default="ubuntu",
    type=UserName,
    help="The user to use when SSH'ing to a client server.")
CRAWL_PARSER.add_argument(
    "-s", "--summarize",
    default=False,
    action="store_true",
    help="If passed, then don't make any changes, but summarize what would "
         "be done.")
CRAWL_PARSER.add_argument(
    "--limit",
    default=0,
    type=int,
    help="Number of URLs to crawl (over all the IP addresses given.) If 0, "
         "then crawl without limit until all URLs are crawled.")
CRAWL_PARSER.add_argument(
    "--binary-path", "-b",
    default=DEFAULT_CRAWL_ARGS.binary_path,
    help="Path to the PageGraph enabled Brave binary for pagegraph-crawl.")
CRAWL_PARSER.add_argument(
    "--s3-bucket",
    default=DEFAULT_CRAWL_ARGS.s3_bucket,
    help="The S3 bucket to write the resulting graphs into.")
CRAWL_PARSER.add_argument(
    "--pagegraph-secs",
    type=int,
    default=DEFAULT_CRAWL_ARGS.pagegraph_secs,
    help="Number of seconds let the page execute before requesting the graph.")
CRAWL_PARSER.add_argument(
    "--client-code-path",
    default=DEFAULT_CLIENT_CODE_PATH,
    help="Path to pagegraph-tranco-crawl code on the client.")
CRAWL_PARSER.add_argument(
    "--client-timeout",
    default=DEFAULT_CRAWL_ARGS.timeout,
    type=int,
    help="Maximum number of seconds to wait on the client before quitting.")
CRAWL_PARSER.add_argument(
    "--timeout",
    default=DEFAULT_CRAWL_ARGS.timeout + 20,
    type=int,
    help="Maximum number of seconds overall to wait before quitting.")
add_logger_argument(CRAWL_PARSER)
CRAWL_PARSER.add_argument(
    "--silent",
    default=False,
    action="store_true",
    help="Suppress all messages and logging.")
CRAWL_PARSER.set_defaults(func=crawl_cmd)

QUERY_PARSER = SUBPARSERS.add_parser(
    "query",
    help="Query a graph using pagegraph-query.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
QUERY_PARSER.add_argument(
    "ip",
    help="The IP addresses of clients to interact with.",
    nargs="+")
QUERY_PARSER.add_argument(
    "-u", "--user",
    default="ubuntu",
    type=UserName,
    help="The user to use when SSH'ing to a client server.")
QUERY_PARSER.add_argument(
    "--limit",
    default=0,
    type=int,
    help="Number of graphs to query (over all the IP addresses given.) If 0, "
         "then crawl without limit until all graphs are queried.")

try:
    ARGS = PARSER.parse_args()
    IPS = [ipaddress.ip_address(x) for x in ARGS.ip]
    RESULT = ARGS.func(ARGS, IPS)
except ValueError as e:
    print(f"Invalid argument: {e}", file=sys.stderr)
    sys.exit(1)
