from concurrent.futures import ThreadPoolExecutor
import sys
import threading

from pgcrawl.types import ClientServer, IPAddress, UserName, WorkItem
from pgcrawl.logging import log, log_error


class ThreadIPManager:
    ip_addresses: list[IPAddress]
    user: UserName
    timeout: int
    quiet: bool
    mapping_dict: dict[threading.Thread, IPAddress] = {}

    def __init__(self, ips: list[IPAddress], user: UserName, timeout: int,
                 quiet: bool) -> None:
        self.ip_addresses = ips
        self.user = user
        self.quiet = quiet
        self.timeout = timeout

    def init_thread(self) -> None:
        thread = threading.current_thread()
        current_index = len(self.mapping_dict)
        self.mapping_dict[thread] = self.ip_addresses[current_index]

    def call_on_thread(self, work: WorkItem) -> tuple[IPAddress, bool]:
        func, message, *args = work
        thread = threading.current_thread()
        ip = self.mapping_dict[thread]
        log(f"{ip}: {message}", self.quiet)
        server_desc = ClientServer(ip, self.user)
        is_success = func(server_desc, *args, timeout=self.timeout,
                          quiet=self.quiet)
        return ip, is_success

    def call(self, executor: ThreadPoolExecutor,
             work_item: WorkItem) -> dict[IPAddress, bool]:
        func, message, *args = work_item
        results = {}
        work_items = [(func, message, *args) for _ in self.ip_addresses]
        for ip, a_result in executor.map(self.call_on_thread, work_items):
            results[ip] = a_result
        return results

    def num_workers(self) -> int:
        return len(self.ip_addresses)


def exit_with_results(func_name: str, results: dict[IPAddress, bool]) -> None:
    any_failures = False
    for ip, a_result in results.items():
        if a_result is False:
            any_failures = True
            log_error(f"Error: {ip}:{func_name}()")
    if any_failures:
        sys.exit(1)
    sys.exit(0)


def is_all_successful(results: dict[IPAddress, bool]) -> bool:
    for a_result in results.values():
        if a_result is False:
            return False
    return True
