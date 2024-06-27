from dataclasses import dataclass
from io import StringIO
from ipaddress import ip_address
from pathlib import Path
import re

from invoke.exceptions import CommandTimedOut

from pgcrawl import GIT_URL
from pgcrawl.dispatch import TODO_DIR, UNDERWAY_DIR, DONE_DIR, ERROR_DIR
from pgcrawl.logging import log, log_error
from pgcrawl.subprocesses import run_ssh_cmd
from pgcrawl.types import IPAddress, ClientServer, TrancoDomain, Url


def activate_env_cmd_str(client_path: str) -> str:
    project_name = Path(client_path).name
    commands = [
        "cd ~/",
        f"cd {client_path}",
        ". ./bin/activate",
        f"cd {project_name}"
    ]
    activate_env_cmd = " && ".join(commands)
    return activate_env_cmd


def test_connection(server: ClientServer, timeout: int = 10,
                    quiet: bool = False) -> bool:
    conn = server.connection()
    expected_user_dir = "/home/" + server.user
    test_cmd = f"test -d {expected_user_dir}"
    if run_ssh_cmd(conn, test_cmd, timeout, quiet):
        conn.close()
        log("   connected successfully!", quiet)
        return True
    return False


def check_client_code(server: ClientServer,
                      client_path: str, timeout: int = 10,
                      quiet: bool = False) -> bool:
    conn = server.connection()
    check_install_cmd = f"test -d {client_path}"

    rs = run_ssh_cmd(conn, check_install_cmd, timeout, quiet)
    conn.close()
    if rs:
        log("   Looks already installed!", quiet)
        return True
    else:
        log("   client code does not seem present", quiet)
        return False


def delete_client_code(server: ClientServer,
                       client_path: str, timeout: int = 10,
                       quiet: bool = False) -> bool:
    conn = server.connection()
    delete_install = f"rm -Rf {client_path}"
    rs = run_ssh_cmd(conn, delete_install, timeout, quiet)
    conn.close()
    if rs:
        log("   installed deleted!", quiet)
        return True
    else:
        log("   error when deleting", quiet)
        return False


def install_client_code(server: ClientServer,
                        client_path: str, timeout: int = 10,
                        quiet: bool = False) -> bool:
    conn = server.connection()

    intended_dest = Path(client_path).name
    commands = [
        f"python3 -m venv {intended_dest}",
        f"cd {intended_dest}",
        f". ./bin/activate",
        f"git clone {GIT_URL} {intended_dest}",
        f"cd ./{intended_dest}",
        f"pip3 install -r requirements.txt"
    ]
    combined_cmd = " && ".join(commands)
    rs = run_ssh_cmd(conn, combined_cmd, timeout, quiet)
    conn.close()
    if rs:
        log("   installed successfully!", quiet)
        return True
    else:
        log("   some error?", quiet)
        return False


def setup_client_code(server: ClientServer,
                      client_path: str, timeout: int = 10,
                      quiet: bool = False) -> bool:
    conn = server.connection()
    log(f"-  attempting to setup project at {client_path}.", quiet)
    setup_cmd = activate_env_cmd_str(client_path)
    setup_cmd += " && ./client-setup.py"
    if quiet:
        setup_cmd += " --quiet"
    rs = run_ssh_cmd(conn, setup_cmd, timeout, quiet)
    if not rs:
        conn.close()
        log("!  but an error occurred!", quiet)
        return False
    return True


def crawl_with_client_server(server: ClientServer, domain: TrancoDomain,
                             client_code_path: str, binary_path: str,
                             pagegraph_secs: int, s3_bucket: str,
                             client_timeout: int, timeout: int,
                             quiet: bool) -> bool:
    conn = server.connection()
    log(f"-  attempting to crawl {domain.url()} with {server.desc()}.", quiet)
    crawl_cmd = "killall -9 brave; sleep 3; "
    crawl_cmd += activate_env_cmd_str(client_code_path)
    crawl_cmd += " && " + " ".join([
        "./client.py",
        "--rank", str(domain.rank),
        "--url", domain.url(),
        "--seconds", str(pagegraph_secs),
        "--timeout", str(client_timeout),
        "--client-code-path", client_code_path,
        "--binary-path", binary_path,
        "--s3-bucket", s3_bucket
    ])
    if quiet:
        crawl_cmd += " --quiet"
    rs = run_ssh_cmd(conn, crawl_cmd, timeout, quiet)
    if not rs:
        conn.close()
        log("!  but an error occurred!", quiet)
        return False
    return True


def domains_to_crawl() -> list[TrancoDomain]:
    domains = []
    for path in TODO_DIR.iterdir():
        if not path.is_file():
            continue
        domains.append(TrancoDomain(path))
    return domains


def record_as_underway(record: TrancoDomain) -> None:
    record.move_to_dir(UNDERWAY_DIR)


def record_as_complete(record: TrancoDomain) -> None:
    record.move_to_dir(DONE_DIR)


def record_as_error(record: TrancoDomain) -> None:
    record.move_to_dir(ERROR_DIR)
