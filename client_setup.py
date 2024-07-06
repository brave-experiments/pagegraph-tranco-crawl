#!/usr/bin/env python3

import argparse
import sys

import pgcrawl
from pgcrawl.client import DIRS_TO_WRITE
import pgcrawl.setup as PG_CRAWL_SETUP
from pgcrawl.logging import add_logger_argument, Logger


PARSER = argparse.ArgumentParser(
    prog=f"{pgcrawl.NAME}: client-setup",
    description="Sets up directory structure and other needed steps for the "
                "clients running a pagegraph crawl.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
add_logger_argument(PARSER)
PARSER.add_argument(
    "--binary", "-b",
    default=None,
    help="The binary to use when checking pagegraph-crawl.")
ARGS = PARSER.parse_args()

QUIET = ARGS.quiet
LOGGER = Logger(ARGS.log_level)

PG_CRAWL_SETUP.mkdirs(DIRS_TO_WRITE, LOGGER)
if not PG_CRAWL_SETUP.check_for_brave_binary(ARGS.binary, LOGGER):
    if not PG_CRAWL_SETUP.install_brave_binary(LOGGER):
        sys.exit(1)

if not PG_CRAWL_SETUP.check_for_pagegraph_crawl(LOGGER):
    if not PG_CRAWL_SETUP.clone_brave_crawl(LOGGER):
        sys.exit(1)

if not PG_CRAWL_SETUP.check_for_pagegraph_query(LOGGER):
    if not PG_CRAWL_SETUP.clone_pagegraph_query(LOGGER):
        sys.exit(1)
