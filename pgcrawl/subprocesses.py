from io import StringIO

from invoke.exceptions import CommandTimedOut

from pgcrawl.logging import Logger
from pgcrawl.types import ClientServer


def run_ssh_cmd(server: ClientServer, cmd: str, timeout: int,
                logger: Logger) -> bool:
    # pylint: disable=broad-exception-caught
    conn = server.connection()
    logger.debug(f"*  calling {conn.user}@{conn.host}: {cmd}")
    try:
        stdout_stream = StringIO()
        stderr_stream = StringIO()
        rs = conn.run(cmd, warn=True, hide=True, out_stream=stdout_stream,
                      err_stream=stderr_stream, timeout=timeout)
        logger.debug(stdout_stream.getvalue())
        if rs.exited != 0:
            logger.error(stdout_stream.getvalue())
            return False
        return True
    except CommandTimedOut as e:
        logger.error("Timeout: " + str(e))
        return False
    except Exception as e:
        logger.error(str(e))
        return False
    finally:
        server.close()
