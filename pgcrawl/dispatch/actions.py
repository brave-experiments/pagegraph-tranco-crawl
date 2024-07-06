from pathlib import Path

from pgcrawl import GIT_URL
from pgcrawl.dispatch import TODO_DIR, UNDERWAY_DIR, DONE_DIR, ERROR_DIR
from pgcrawl.logging import Logger
from pgcrawl.subprocesses import run_ssh_cmd
from pgcrawl.types import ClientServer, TrancoDomain


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


def test_connection(server: ClientServer, timeout: int,
                    logger: Logger) -> bool:
    expected_user_dir = "/home/" + server.user
    test_cmd = f"test -d {expected_user_dir}"
    if run_ssh_cmd(server, test_cmd, timeout, logger):
        logger.debug("   connected successfully!")
        return True
    return False


def check_client_code(server: ClientServer,
                      client_path: str, timeout: int,
                      logger: Logger) -> bool:
    check_install_cmd = f"test -d {client_path}"
    rs = run_ssh_cmd(server, check_install_cmd, timeout, logger)
    if rs:
        logger.debug("   Looks already installed!")
        return True
    logger.debug("   client code does not seem present",)
    return False


def delete_client_code(server: ClientServer,
                       client_path: str, timeout: int,
                       logger: Logger) -> bool:
    delete_install = f"rm -Rf {client_path}"
    rs = run_ssh_cmd(server, delete_install, timeout, logger)
    if rs:
        logger.debug("   installed deleted!")
        return True
    logger.debug("   error when deleting")
    return False


def install_client_code(server: ClientServer,
                        client_path: str, timeout: int,
                        logger: Logger) -> bool:
    intended_dest = Path(client_path).name
    commands = [
        f"python3 -m venv {intended_dest}",
        f"cd {intended_dest}",
        ". ./bin/activate",
        f"git clone {GIT_URL} {intended_dest}",
        f"cd ./{intended_dest}",
        "pip3 install -r requirements.txt"
    ]
    combined_cmd = " && ".join(commands)
    rs = run_ssh_cmd(server, combined_cmd, timeout, logger)
    if rs:
        logger.debug("   installed successfully!")
        return True
    logger.debug("   some error?")
    return False


def setup_client_code(server: ClientServer,
                      client_path: str, timeout: int,
                      logger: Logger) -> bool:
    logger.debug(f"-  attempting to setup project at {client_path}.")
    setup_cmd = activate_env_cmd_str(client_path)
    setup_cmd += " && ./client_setup.py " + logger.to_arg()
    rs = run_ssh_cmd(server, setup_cmd, timeout, logger)
    if not rs:
        logger.debug("!  but an error occurred!")
        return False
    return True


def kill_child_processes(server: ClientServer, timeout: int,
                         logger: Logger) -> bool:
    logger.debug("-  attempting to kill child processes.")
    kill_cmd = "killall -9 brave; killall Xvfb;"
    rs = run_ssh_cmd(server, kill_cmd, timeout, logger)
    if not rs:
        logger.debug("!  but an error occurred!")
        return False
    return True


def crawl_with_client_server(server: ClientServer, domain: TrancoDomain,
                             client_code_path: str, binary_path: str,
                             pagegraph_secs: int, s3_bucket: str,
                             client_timeout: int, timeout: int,
                             logger: Logger) -> bool:
    logger.debug(f"-  crawling {domain.url()} with {server.desc()}.")
    crawl_cmd = "killall -9 brave; killall Xvfb; sleep 3; "
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
    crawl_cmd += logger.to_arg()
    rs = run_ssh_cmd(server, crawl_cmd, timeout, logger)
    if not rs:
        logger.debug("!  but an error occurred!")
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
