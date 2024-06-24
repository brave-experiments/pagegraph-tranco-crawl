from ipaddress import ip_address, IPv4Address, IPv6Address
import json
from pathlib import Path

from fabric import Connection

from pgcrawl.consts import DEFAULT_IP_FILE
from pgcrawl.logging import log, error


def test_connection(user: str, ip: IPv4Address | IPv6Address,
                    quiet: bool = False) -> bool:
    expected_user_dir = "/home/" + user
    test_cmd = f"test -d {expected_user_dir}"
    try:
        log(f"Testing existence of to {user}@{ip}:{expected_user_dir}")
        rs = Connection(host=str(ip), user=user).run(test_cmd)
        log(rs.stdout, quiet)
        if rs.exited != 0:
            error(rs.stderr)
            return False
        else:
            log("...connected successfully!")
            return True
    except Exception as e:
        error(str(e))
        return False


def install_client_code(user: str, ip: IPv4Address | IPv6Address,
                        client_path: str,
                        quiet: bool = False) -> bool:
    return True


def read_ips(path: None | Path = None,
             quiet: bool = False) -> list[IPv4Address | IPv6Address]:
    if path:
        ip_path = path
    else:
        ip_path = DEFAULT_IP_FILE
    lines = ip_path.read_text().split("\n")
    ips = []
    for a_line in lines:
        try:
            ips.append(ip_address(a_line.strip()))
        except ValueError:
            log(f"Ignoring line: {a_line}", quiet)
    return ips
