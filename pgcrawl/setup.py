import pathlib

from pgcrawl.logging import Logger
from pgcrawl import PG_CRAWL_GIT_URL, BRAVE_INSTALL_SCRIPT
from pgcrawl.client import PAGEGRAPH_QUERY_ENV_DIR, PAGEGRAPH_QUERY_PROJECT_DIR
from pgcrawl.client import PAGEGRAPH_CRAWL_DIR
from pgcrawl.subprocesses import call


def mkdirs(dirs: list[pathlib.Path], logger: Logger) -> None:
    logger.debug("Setting up the crawling workspace.")
    for a_dir in dirs:
        try:
            a_dir.mkdir()
            logger.info(f"Created {a_dir}")
        except FileExistsError:
            logger.info(f"Directory {a_dir} already exists")


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


def check_for_pagegraph_crawl(logger: Logger) -> bool:
    path = PAGEGRAPH_CRAWL_DIR
    pg_crawl_check_args = ["test", "-d", str(path)]
    pg_crawl_gitinfo_check_args = ["test", "-f", str(path / ".git" / "config")]
    all_args = (pg_crawl_check_args, pg_crawl_gitinfo_check_args)
    for args in all_args:
        if not call(args, logger):
            return False
    return True


def clone_brave_crawl(logger: Logger) -> bool:
    path = PAGEGRAPH_CRAWL_DIR
    clone_args = ["git", "clone", PG_CRAWL_GIT_URL, str(path)]
    if not call(clone_args, logger):
        return False
    install_args = ["npm", "install"]
    return call(install_args, logger, cwd=str(path))


def check_for_pagegraph_query(logger: Logger) -> bool:
    requirements_file_path = PAGEGRAPH_QUERY_PROJECT_DIR / "requirements.txt"
    check_cmd_args = ["test", "-d", str(requirements_file_path)]
    return call(check_cmd_args, logger)


def clone_pagegraph_query(logger: Logger) -> bool:
    venv_args = ["python3", "-m", "venv", str(PAGEGRAPH_QUERY_ENV_DIR)]
    if not call(venv_args, logger):
        return False

    clone_args = ["git", "clone", PG_CRAWL_GIT_URL,
                  str(PAGEGRAPH_QUERY_PROJECT_DIR)]
    if not call(clone_args, logger):
        return False

    env_activate_path = PAGEGRAPH_QUERY_ENV_DIR / "bin/activate"
    activate_args = ["source", str(env_activate_path)]
    if not call(activate_args, logger):
        return False

    install_args = ["pip3", "install", "-r", "requirements.txt"]
    install_cwd_path = PAGEGRAPH_QUERY_PROJECT_DIR
    if not call(install_args, logger, cwd=install_cwd_path):
        return False
    return True
