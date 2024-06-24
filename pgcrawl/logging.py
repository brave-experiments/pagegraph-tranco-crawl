import sys


def as_string(msg: str | bytes) -> str | None:
    if isinstance(msg, bytes):
        text = msg.decode("utf8")
    else:
        text = msg
    if text.strip() == "":
        return None
    return text


def log(msg: str | bytes, quiet: bool = False) -> None:
    if text := as_string(msg):
        if not quiet:
            print(text)


def error(msg: str | bytes) -> None:
    if text := as_string(msg):
        print(text, file=sys.stderr)
