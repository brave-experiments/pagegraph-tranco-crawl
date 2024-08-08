from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import threading
from typing import Iterable

from pgcrawl.types import ClientServer, IPAddress, UserName, WorkItem
from pgcrawl.types import WorkResponse
from pgcrawl.logging import Logger


class ThreadIPManager:
    ip_addresses: list[IPAddress]
    user: UserName
    timeout: int
    logger: Logger
    mapping_dict: dict[threading.Thread, IPAddress] = {}

    def __init__(self, ips: list[IPAddress], user: UserName, timeout: int,
                 logger: Logger) -> None:
        self.ip_addresses = ips
        self.user = user
        self.logger = logger
        self.timeout = timeout

    def init_thread(self) -> None:
        thread = threading.current_thread()
        current_index = len(self.mapping_dict)
        self.mapping_dict[thread] = self.ip_addresses[current_index]

    def call_on_thread(self, work: WorkItem) -> tuple[IPAddress, bool]:
        # pylint: disable=broad-exception-caught
        func = work.func
        args = work.args
        thread = threading.current_thread()
        ip = self.mapping_dict[thread]
        self.logger.info(f"({ip}) -> {work.message}")
        server_desc = ClientServer(ip, self.user)
        try:
            is_success = func(server_desc, *args, timeout=self.timeout,
                              logger=self.logger)
        except Exception as e:
            self.logger.error(f"({ip}) -> {e}")
            is_success = False
        finally:
            server_desc.close()
        return ip, is_success

    def call_on_each(self, executor: ThreadPoolExecutor,
                     work_item: WorkItem) -> list[WorkResponse]:
        work_items = [work_item for _ in self.ip_addresses]
        results = []
        for work_response in self.call(executor, work_items):
            results.append(work_response)
        return results

    def call(self, executor: ThreadPoolExecutor,
             work_items: list[WorkItem]) -> Iterable[WorkResponse]:
        futures_map = {}
        for work_item in work_items:
            future = executor.submit(self.call_on_thread, work_item)
            futures_map[future] = work_item

        for a_future in as_completed(futures_map):
            work_item = futures_map[a_future]
            ip_address, was_success = a_future.result()
            yield WorkResponse(ip_address, was_success, work_item)

    def num_workers(self) -> int:
        return len(self.ip_addresses)


def exit_with_results(func_name: str, results: list[WorkResponse],
                      logger: Logger) -> None:
    any_failures = False
    for work_response in results:
        if work_response.is_success is False:
            any_failures = True
            logger.error(f"({work_response.ip}) -> {func_name}()")
    if any_failures:
        sys.exit(1)
    sys.exit(0)


def is_all_successful(results: list[WorkResponse]) -> bool:
    for work_response in results:
        if work_response.is_success is False:
            return False
    return True
