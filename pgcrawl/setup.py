import pathlib
import subprocess
import sys
from typing import Any

from pgcrawl.logging import Logger
from pgcrawl import PG_CRAWL_GIT_URL, BRAVE_INSTALL_SCRIPT


def mkdirs(dirs: list[pathlib.Path], logger: Logger) -> None:
    logger.debug(f"Setting up the crawling workspace.")
    for a_dir in dirs:
        try:
            a_dir.mkdir()
            logger.info(f"Created {a_dir}")
        except FileExistsError:
            logger.info(f"Directory {a_dir} already exists")
            pass


def call(args: list[str], logger: Logger, **kwargs: Any) -> bool:
    logger.debug("Shell: " + " ".join(args))
    rs = subprocess.run(args, capture_output=True, **kwargs)
    logger.debug(rs.stdout)
    if rs.returncode == 0:
        return True
    else:
        logger.error(rs.stderr)
        return False


def check_for_brave_binary(binary_path: str | None,
                           logger: Logger) -> bool:
    if binary_path:
        check_args = ["test", "-f", binary_path]
    else:
        check_args = ["which", "brave-browser-nightly"]
    return call(check_args, logger)


def install_brave_binary(logger: Logger) -> bool:
    install_args = ["sudo", str(BRAVE_INSTALL_SCRIPT)]
    return call(install_args, logger)


def check_for_pagegraph_crawl(path: pathlib.Path, logger: Logger) -> bool:
    pg_crawl_check_args = ["test", "-d", str(path)]
    pg_crawl_gitinfo_check_args = ["test", "-f", str(path / ".git" / "config")]
    all_args = (pg_crawl_check_args, pg_crawl_gitinfo_check_args)
    for args in all_args:
        if not call(args, logger):
            return False
    return True


def clone_brave_crawl(path: pathlib.Path, logger: Logger) -> bool:
    clone_args = ["git", "clone", PG_CRAWL_GIT_URL, str(path)]
    if not call(clone_args, logger):
        return False
    install_args = ["npm", "install"]
    return call(install_args, logger, cwd=str(path))
