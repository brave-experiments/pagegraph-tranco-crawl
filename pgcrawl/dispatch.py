from io import StringIO
from ipaddress import ip_address
import json
from pathlib import Path

from fabric import Connection
from invoke.exceptions import CommandTimedOut

from pgcrawl import GIT_URL, IPAddress
from pgcrawl.logging import log, log_error


def run(conn: Connection, cmd: str, timeout: int, quiet: bool) -> bool:
    log(f"*  calling {conn.user}@{conn.host}: {cmd}", quiet)
    try:
        stdout_stream = StringIO()
        stderr_stream = StringIO()
        rs = conn.run(cmd, warn=True, hide=True, out_stream=stdout_stream,
                      err_stream=stderr_stream, timeout=timeout)
        log(stdout_stream.getvalue(), quiet)
        if rs.exited != 0:
            log_error(stdout_stream.getvalue())
            return False
        return True
    except CommandTimedOut as e:
        log_error("Timeout: " + str(e))
        return False
    except Exception as e:
        log_error(str(e))
        return False


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


def test_connection(ip: IPAddress, user: str,
                    timeout: int, quiet: bool = False) -> bool:
    conn = Connection(host=str(ip), user=user)
    expected_user_dir = "/home/" + user
    test_cmd = f"test -d {expected_user_dir}"
    if run(conn, test_cmd, timeout, quiet):
        conn.close()
        log("   connected successfully!", quiet)
        return True
    return False


def check_client_code(ip: IPAddress, user: str,
                      client_path: str, timeout: int,
                      quiet: bool = False) -> bool:
    conn = Connection(host=str(ip), user=user)
    check_install_cmd = f"test -d {client_path}"

    rs = run(conn, check_install_cmd, timeout, quiet)
    conn.close()
    if rs:
        log("   Looks already installed!", quiet)
        return True
    else:
        log("   client code does not seem present", quiet)
        return False


def delete_client_code(ip: IPAddress, user: str,
                       client_path: str, timeout: int,
                       quiet: bool = False) -> bool:
    conn = Connection(host=str(ip), user=user)
    delete_install = f"rm -Rf {client_path}"
    rs = run(conn, delete_install, timeout, quiet)
    conn.close()
    if rs:
        log("   installed deleted!", quiet)
        return True
    else:
        log("   error when deleting", quiet)
        return False


def install_client_code(ip: IPAddress, user: str,
                        client_path: str, timeout: int,
                        quiet: bool = False) -> bool:
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
    conn = Connection(host=str(ip), user=user)
    rs = run(conn, combined_cmd, timeout, quiet)
    conn.close()
    if rs:
        log("   installed successfully!", quiet)
        return True
    else:
        log("   some error?", quiet)
        return False


def setup_client_code(ip: IPAddress, user: str,
                      client_path: str, timeout: int,
                      quiet: bool = False) -> bool:
    log(f"-  attempting to setup project at {client_path}.", quiet)
    conn = Connection(host=str(ip), user=user)
    setup_cmd = activate_env_cmd_str(client_path)
    setup_cmd += " && ./client-setup.py"
    if quiet:
        setup_cmd += " --quiet"
    rs = run(conn, setup_cmd, timeout, quiet)
    if not rs:
        conn.close()
        log("!  but an error occurred!", quiet)
        return False
    return True
