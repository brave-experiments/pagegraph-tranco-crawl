from argparse import ArgumentParser
from enum import StrEnum
import sys
from typing import Any


class LoggingLevel(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    ERROR = "error"


def as_string(msg: Any) -> str | None:
    match msg:
        case str():
            text = msg
        case bytes():
            text = msg.decode("utf8")
        case _:
            text = str(msg)
    if text.strip() == "":
        return None
    return text


def add_logger_argument(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "error"],
        help="What message cutoff to use when logging and providing feedback.")


class Logger:
    level: LoggingLevel = LoggingLevel.INFO
    print_debug: bool
    print_info: bool

    def __init__(self, log_level: str) -> None:
        self.level = LoggingLevel(log_level)
        self.print_debug = self.level == LoggingLevel.DEBUG
        self.print_info = self.level in [LoggingLevel.DEBUG, LoggingLevel.INFO]

    def to_arg(self) -> str:
        return f" --log-level {self.level.value}"

    def debug(self, msg: Any) -> bool:
        if self.print_debug:
            if text := as_string(msg):
                print(f"DEBUG: {text}")
                return True
        return False

    def info(self, msg: Any) -> bool:
        if self.print_info:
            if text := as_string(msg):
                print(f"INFO: {text}")
                return True
        return False

    def error(self, msg: Any) -> bool:
        if text := as_string(msg):
            print(f"ERROR: {text}", file=sys.stderr)
            return True
        return False
