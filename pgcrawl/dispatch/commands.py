from concurrent.futures import ThreadPoolExecutor
from enum import Enum, auto
from typing import cast, Callable, Any

from pgcrawl.client.args import ClientCrawlArgs
from pgcrawl.dispatch.actions import test_connection, delete_client_code
from pgcrawl.dispatch.actions import install_client_code, check_client_code
from pgcrawl.dispatch.actions import setup_client_code, domains_to_crawl
from pgcrawl.dispatch.actions import crawl_with_client_server
from pgcrawl.dispatch.actions import record_as_complete, record_as_error
from pgcrawl.logging import log, log_error
from pgcrawl.threading import ThreadIPManager, exit_with_results
from pgcrawl.threading import is_all_successful
from pgcrawl.types import IPAddress, UserName, WorkItem, TrancoDomain


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
            work_item = WorkItem(test_connection, message, [])
            rs = manager.call_on_each(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("test_connection", rs)

        if Action.DELETE_CLIENT_CODE in actions or do_all_actions:
            message = f"Deleting client code from {client_path}"
            log(message, quiet)
            work_item = WorkItem(delete_client_code, message, [client_path])
            rs = manager.call_on_each(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("delete_client_code", rs)

        if Action.INSTALL_CLIENT_CODE in actions or do_all_actions:
            message = f"Installing client code at {client_path}"
            log(message, quiet)
            work_item = WorkItem(install_client_code, message, [client_path])
            rs = manager.call_on_each(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("install_client_code", rs)

        if Action.CHECK_CLIENT_CODE in actions or do_all_actions:
            message = f"Checking if client code is installed at {client_path}"
            log(message, quiet)
            work_item = WorkItem(check_client_code, message, [client_path])
            rs = manager.call_on_each(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("check_client_code", rs)

        if Action.SETUP_CLIENT_CODE in actions or do_all_actions:
            message = f"Setting up client code at {client_path}"
            log(message, quiet)
            work_item = WorkItem(setup_client_code, message, [client_path])
            rs = manager.call_on_each(executor, work_item)
            if not is_all_successful(rs):
                exit_with_results("setup_client_code", rs)


def client_crawl(ips: list[IPAddress], user: UserName, summarize: bool,
                 limit: int, client_crawl_args: ClientCrawlArgs,
                 timeout: int, quiet: bool) -> None:
    domains = domains_to_crawl()
    if limit > 0:
        domains = domains[:limit]

    if summarize:
        log(f"Would crawl {len(domains)} domains with {len(ips)} servers",
            False)
        return

    manager = ThreadIPManager(ips, user, timeout, quiet)
    with ThreadPoolExecutor(max_workers=manager.num_workers(),
                            initializer=manager.init_thread,) as executor:
        message = f"Crawling {len(domains)} domains"
        log(message, quiet)

        work_items = []

        for tranco_record in domains:
            work_msg = f"Crawling {str(tranco_record)} domains"
            work_args = [
                tranco_record,
                client_crawl_args.client_code_path,
                client_crawl_args.binary_path,
                client_crawl_args.pagegraph_secs,
                client_crawl_args.s3_bucket,
                client_crawl_args.timeout
            ]
            work_item = WorkItem(crawl_with_client_server, work_msg, work_args)
            work_items.append(work_item)

        for work_response in manager.call(executor, work_items):
            ip = work_response.ip
            func = work_response.work_item.func
            work_args = work_response.work_item.args
            tranco_record = cast(TrancoDomain, work_args[0])
            work_args_str = " ".join([str(x) for x in work_args])
            if not work_response.is_success:
                record_as_error(tranco_record)
                log_error(str(work_response))
            else:
                record_as_complete(tranco_record)
                log(str(work_response))
