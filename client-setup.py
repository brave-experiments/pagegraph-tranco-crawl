#!/usr/bin/env python3

import argparse
import pathlib
import sys

import pgcrawl.setup as PG_CRAWL_SETUP


PARSER = argparse.ArgumentParser(
    prog="pagegraph-tranco-crawl client setup",
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

WORKSPACE_DIR = pathlib.Path("./workspace")

CLIENT_DIR = WORKSPACE_DIR / "client"
RECEIVED_DIR = CLIENT_DIR / "received"
CRAWLING_START_DIR = CLIENT_DIR / "crawling-start"
CRAWLING_ERROR_DIR = CLIENT_DIR / "crawling-error"
AWS_START = CLIENT_DIR / "aws-start"
AWS_ERROR_DIR = CLIENT_DIR / "aws-error"
COMPLETE_DIR = CLIENT_DIR / "complete"
ERROR_DIR = CLIENT_DIR / "error"

DIRS_TO_WRITE = [
    WORKSPACE_DIR,
    CLIENT_DIR,
    RECEIVED_DIR,
    CRAWLING_START_DIR,
    CRAWLING_ERROR_DIR,
    AWS_START,
    AWS_ERROR_DIR,
    COMPLETE_DIR,
    ERROR_DIR
]

PG_CRAWL_SETUP.mkdirs(DIRS_TO_WRITE, QUIET)
if not PG_CRAWL_SETUP.check_for_brave_binary(ARGS.binary, QUIET):
    if not PG_CRAWL_SETUP.install_brave_binary(QUIET):
        sys.exit(1)

PAGEGRAPH_CRAWL_DIR = CLIENT_DIR / "pagegraph-crawl"

if not PG_CRAWL_SETUP.check_for_pagegraph_crawl(PAGEGRAPH_CRAWL_DIR, QUIET):
    if not PG_CRAWL_SETUP.clone_brave_crawl(PAGEGRAPH_CRAWL_DIR, QUIET):
        sys.exit(1)
