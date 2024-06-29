from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
import re
from typing import Any, Callable, Optional

from fabric import Connection

from pgcrawl.logging import Logger


Url = str
UserName = str
IPAddress = IPv4Address | IPv6Address


@dataclass
class WorkItem:
    func: Callable[..., bool]
    message: str
    args: list[Any]

    def desc(self) -> str:
        args_str = ", ".join([str(x) for x in self.args])
        return f"{self.func.__name__}({args_str})"


@dataclass
class WorkResponse:
    ip: IPAddress
    is_success: bool
    work_item: WorkItem

    def log(self, logger: Logger) -> None:
        if self.is_success:
            logger.info(self)
        else:
            logger.error(self)

    def __str__(self) -> str:
        return f"{self.ip} -> {self.work_item.desc()}"


@dataclass
class ClientServer:
    ip: IPAddress
    user: UserName
    conn_: Optional[Connection] = None

    def connection(self) -> Connection:
        if self.conn_:
            return self.conn_
        self.conn_ = Connection(host=str(self.ip), user=self.user)
        return self.conn_

    def close(self) -> bool:
        if self.conn_:
            self.conn_.close()
            self.conn_ = None
            return True
        return False

    def desc(self) -> str:
        return f"{self.user}@{str(self.ip)}"


class TrancoDomain:
    file: Path
    rank: int
    domain: str

    def __init__(self, filepath: Path) -> None:
        self.file = filepath
        self.rank, self.domain = self.parse()

    def __str__(self) -> str:
        return str(self.file)

    def parse(self) -> tuple[int, str]:
        match = re.match(r"([0-9]+)_(.*)", self.file.name)
        if not match:
            raise ValueError(f"Invalid tranco record: {self.file}")
        return int(match.group(1)), match.group(2)

    def url(self) -> Url:
        return f"https://{self.domain}"

    def move_to_dir(self, new_dir: Path) -> None:
        new_path = new_dir / self.file.name
        self.file.rename(str(new_path))
        self.file = new_path
