#!/usr/bin/env python3

import argparse
from concurrent.futures import ThreadPoolExecutor
import ipaddress
import multiprocessing
import pathlib
import sys
import threading
from typing import Any, Callable

from pgcrawl import IPAddress, DEFAULT_CLIENT_CODE_PATH, NAME
import pgcrawl.dispatch as PG_DISPATCH
from pgcrawl.logging import log, log_error


PARSER = argparse.ArgumentParser(
    prog=f"{NAME}: dispatch",
    description="Script responsible for dispatching and coordinating calls "
                "to child SSH server.\n"
                "The full set of steps to take to set up a client from "
                "scratch are: --test-connection, --install-client-code, "
                "--check-client-code, --setup-client-code",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
PARSER.add_argument(
    "ip",
    help="The IP addresses of clients to interact with.",
    nargs="+")
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
    "--full-setup",
    default=False,
    action="store_true",
    help="Run all commands needed to setup a client (i.e., --test-connection, "
         "--delete-client-code, --install-client-code, --check-client-code, "
         "--setup-client-code).")
PARSER.add_argument(
    "--client-code-path",
    default=DEFAULT_CLIENT_CODE_PATH,
    help="Path to where this code should be installed on the client.")
PARSER.add_argument(
    "-u", "--user",
    default="ubuntu",
    help="The user to use when SSH'ing to a client server.")
PARSER.add_argument(
    "--crawl",
    default=False,
    action="store_true",
    help="Crawl the given URL .")
PARSER.add_argument(
    "-t", "--timeout",
    default=120,
    type=int,
    help="Maximum number of seconds to wait on a client to do anything.")
PARSER.add_argument(
    "--quiet", "-q",
    default=False,
    action="store_true",
    help="Suppress non-error messages and logging.")

ARGS = PARSER.parse_args()
QUIET = ARGS.quiet
USER = ARGS.user
CLIENT_PATH = ARGS.client_code_path

IPS = [ipaddress.ip_address(x) for x in ARGS.ip]


THREAD_IP_DICT: dict[threading.Thread, IPAddress] = {}


def init_thread(ips: list[IPAddress]) -> None:
    thread = threading.current_thread()
    current_index = len(THREAD_IP_DICT)
    THREAD_IP_DICT[thread] = ips[current_index]


WorkItem = tuple[str, Callable[..., bool], list[Any]]


def do_thread_work(work: WorkItem) -> tuple[IPAddress, bool]:
    message, func, *args = work
    thread = threading.current_thread()
    ip_address = THREAD_IP_DICT[thread]
    log(f"{ip_address}: {message}", QUIET)
    return ip_address, func(ip_address, *args, timeout=ARGS.timeout,
                            quiet=QUIET)


def call_on_all(executor: ThreadPoolExecutor, message: str,
                func: Callable[..., bool],
                *args: list[Any]) -> dict[IPAddress, bool]:
    log(message, QUIET)
    results = {}
    work_items = [(message, func, *args) for _ in IPS]
    for ip, a_result in executor.map(do_thread_work, work_items):
        results[ip] = a_result
    log(f"Results: {results}", QUIET)
    return results


def all_successful(results: dict[IPAddress, bool]) -> bool:
    for a_result in results.values():
        if a_result is False:
            return False
    return True


def exit_with_results(func_name: str, results: dict[IPAddress, bool]) -> None:
    any_failures = False
    for ip, a_result in results.items():
        if a_result is False:
            any_failures = True
            log_error(f"Error: {ip}:{func_name}()")
    if any_failures:
        sys.exit(1)
    sys.exit(0)


with ThreadPoolExecutor(max_workers=len(IPS), initializer=init_thread,
                        initargs=(IPS,)) as executor:
    if ARGS.test_connection or ARGS.full_setup:
        rs = call_on_all(executor, "Checking connections",
                         PG_DISPATCH.test_connection, USER)
        if not all_successful(rs):
            exit_with_results("test_connection", rs)

    if ARGS.delete_client_code or ARGS.full_setup:
        rs = call_on_all(executor, f"Deleting client code from {CLIENT_PATH}",
                         PG_DISPATCH.delete_client_code, USER, CLIENT_PATH)
        if not all_successful(rs):
            exit_with_results("delete_client_code", rs)

    if ARGS.install_client_code or ARGS.full_setup:
        rs = call_on_all(executor, f"Installing client code at {CLIENT_PATH}",
                         PG_DISPATCH.install_client_code, USER, CLIENT_PATH)
        if not all_successful(rs):
            exit_with_results("install_client_code", rs)

    if ARGS.check_client_code or ARGS.full_setup:
        rs = call_on_all(
            executor, f"Checking if client code is installed at {CLIENT_PATH}",
            PG_DISPATCH.check_client_code, USER, CLIENT_PATH)
        if not all_successful(rs):
            exit_with_results("check_client_code", rs)

    if ARGS.setup_client_code or ARGS.full_setup:
        rs = call_on_all(executor, f"Setting up client code at {CLIENT_PATH}",
                         PG_DISPATCH.setup_client_code, USER, CLIENT_PATH)
        if not all_successful(rs):
            exit_with_results("setup_client_code", rs)
