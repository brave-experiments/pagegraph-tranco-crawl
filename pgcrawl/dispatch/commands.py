from concurrent.futures import ThreadPoolExecutor
from enum import Enum, auto

from pgcrawl.dispatch import test_connection, delete_client_code
from pgcrawl.dispatch import install_client_code, check_client_code
from pgcrawl.dispatch import setup_client_code, domains_to_crawl, crawl
from pgcrawl.logging import log, log_error
from pgcrawl.threading import ThreadIPManager, exit_with_results
from pgcrawl.threading import is_all_successful
from pgcrawl.types import IPAddress, UserName, WorkItem


class Action(Enum):
    TEST_CONNECTION = auto()
    DELETE_CLIENT_CODE = auto()
    INSTALL_CLIENT_CODE = auto()
    CHECK_CLIENT_CODE = auto()
    SETUP_CLIENT_CODE = auto()
    ALL = auto()


def client_setup(actions: list[Action], ips: list[IPAddress],
                 user: UserName, client_path: str, timeout: int,
                 quiet: bool) -> None:
    manager = ThreadIPManager(ips, user, timeout, quiet)
    do_all_actions = Action.ALL in actions
    with ThreadPoolExecutor(max_workers=manager.num_workers(),
                            initializer=manager.init_thread,) as executor:
        if Action.TEST_CONNECTION in actions or do_all_actions:
            message = "Checking connections"
            log(message, quiet)
            work_item: WorkItem = (test_connection, message, [])
            rs = manager.call(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("test_connection", rs)

        if Action.DELETE_CLIENT_CODE in actions or do_all_actions:
            message = f"Deleting client code from {client_path}"
            log(message, quiet)
            work_item = delete_client_code, message, [client_path]
            rs = manager.call(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("delete_client_code", rs)

        if Action.INSTALL_CLIENT_CODE in actions or do_all_actions:
            message = f"Installing client code at {client_path}"
            log(message, quiet)
            work_item = (install_client_code, message, [client_path])
            rs = manager.call(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("install_client_code", rs)

        if Action.CHECK_CLIENT_CODE in actions or do_all_actions:
            message = f"Checking if client code is installed at {client_path}"
            log(message, quiet)
            work_item = (check_client_code, message, [client_path])
            rs = manager.call(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("check_client_code", rs)

        if Action.SETUP_CLIENT_CODE in actions or do_all_actions:
            message = f"Setting up client code at {client_path}"
            log(message, quiet)
            work_item = (setup_client_code, message, [client_path])
            rs = manager.call(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("setup_client_code", rs)


def client_crawl(ips: list[IPAddress], summarize: bool, limit: int,
                 user: UserName, binary_path: str, client_path: str,
                 s3_bucket: str, pg_time: int, max_time: int,
                 quiet: bool) -> None:
    domains = domains_to_crawl()
    if limit > 0:
        domains = domains[:limit]

    if summarize:
        log(f"Would crawl {len(domains)} with {len(ips)} servers", False)
        return

    manager = ThreadIPManager(ips, user, max_time, quiet)
    with ThreadPoolExecutor(max_workers=manager.num_workers(),
                            initializer=manager.init_thread,) as executor:
        message = f"Crawling {len(domains)} domains"
        log(message, quiet)
        for domain in domains:
            work_item = (crawl, message, domain, pg_time, max_time,
                         binary_path, client_path, s3_bucket, quiet)
