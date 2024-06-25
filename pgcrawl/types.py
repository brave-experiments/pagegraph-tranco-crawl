from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
import re
from typing import Any, Callable

from fabric import Connection


Url = str
UserName = str
IPAddress = IPv4Address | IPv6Address
WorkItem = tuple[Callable[..., bool], str, list[Any]]


@dataclass
class ClientServer:
    ip: IPAddress
    user: UserName

    def connection(self) -> Connection:
        return Connection(host=str(self.ip), user=self.user)

    def desc(self) -> str:
        return f"{self.user}@{str(self.ip)}"


class TrancoDomain:
    file: Path
    rank: int
    domain: str

    def __init__(self, filepath: Path) -> None:
        self.file = filepath
        self.rank, self.domain = self.parse()

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
