import pathlib
import subprocess
import sys
from typing import Any

from pgcrawl.logging import log, error
from pgcrawl.consts import PG_CRAWL_GIT_URL, BRAVE_INSTALL_SCRIPT


def mkdirs(dirs: list[pathlib.Path], quiet: bool = False) -> None:
    log(f"Setting up the crawling workspace.", quiet)
    for a_dir in dirs:
        try:
            a_dir.mkdir()
            log(f"Created {a_dir}", quiet)
        except FileExistsError:
            log(f"Directory {a_dir} already exists", quiet)
            pass


def call(args: list[str], quiet: bool = False, **kwargs: Any) -> bool:
    log("Shell: " + " ".join(args), quiet)
    rs = subprocess.run(args, capture_output=True, **kwargs)
    log(rs.stdout, quiet)
    if rs.returncode == 0:
        return True
    else:
        error(rs.stderr)
        return False


def check_for_brave_binary(binary_path: str | None,
                           quiet: bool = False) -> bool:
    if binary_path:
        check_args = ["test", "-f", binary_path]
    else:
        check_args = ["which", "brave-browser-nightly"]
    return call(check_args, quiet)


def install_brave_binary(quiet: bool = False) -> bool:
    install_args = ["sudo", str(BRAVE_INSTALL_SCRIPT)]
    return call(install_args, quiet)


def check_for_pagegraph_crawl(path: pathlib.Path, quiet: bool = False) -> bool:
    pg_crawl_check_args = ["test", "-d", str(path)]
    pg_crawl_gitinfo_check_args = ["test", "-f", str(path / ".git" / "config")]
    all_args = (pg_crawl_check_args, pg_crawl_gitinfo_check_args)
    for args in all_args:
        if not call(args, quiet):
            return False
    return True


def clone_brave_crawl(path: pathlib.Path, quiet: bool = False) -> bool:
    clone_args = ["git", "clone", PG_CRAWL_GIT_URL, str(path)]
    if not call(clone_args, quiet):
        return False
    install_args = ["npm", "install"]
    return call(install_args, quiet, cwd=str(path))
