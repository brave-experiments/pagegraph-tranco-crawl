#!/usr/bin/env python3

import argparse
import pathlib
import sys

import pgcrawl
from pgcrawl.client import PAGEGRAPH_CRAWL_DIR, DIRS_TO_WRITE
import pgcrawl.setup as PG_CRAWL_SETUP


PARSER = argparse.ArgumentParser(
    prog=f"{pgcrawl.NAME}: client-setup",
    description="Sets up directory structure and other needed steps for the "
                "clients running a pagegraph crawl.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
PARSER.add_argument(
    "--quiet", "-q",
    default=False,
    action="store_true",
    help="Suppress non-error messages and logging.")
PARSER.add_argument(
    "--binary", "-b",
    default=None,
    help="The binary to use when checking pagegraph-crawl.")
ARGS = PARSER.parse_args()

QUIET = ARGS.quiet

PG_CRAWL_SETUP.mkdirs(DIRS_TO_WRITE, QUIET)
if not PG_CRAWL_SETUP.check_for_brave_binary(ARGS.binary, QUIET):
    if not PG_CRAWL_SETUP.install_brave_binary(QUIET):
        sys.exit(1)

if not PG_CRAWL_SETUP.check_for_pagegraph_crawl(PAGEGRAPH_CRAWL_DIR, QUIET):
    if not PG_CRAWL_SETUP.clone_brave_crawl(PAGEGRAPH_CRAWL_DIR, QUIET):
        sys.exit(1)
