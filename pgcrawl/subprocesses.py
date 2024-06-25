from io import StringIO

from fabric import Connection
from invoke.exceptions import CommandTimedOut

from pgcrawl.logging import log, log_error


def run_ssh_cmd(conn: Connection, cmd: str, timeout: int, quiet: bool) -> bool:
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
