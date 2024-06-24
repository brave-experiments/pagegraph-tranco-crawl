#!/usr/bin/env python3

import argparse
import sys

import pgcrawl
from pgcrawl.client import BINARY_PATH, CrawlRequest, go


PARSER = argparse.ArgumentParser(
    prog=f"{pgcrawl.NAME}: client",
    description="Script responsible for crawling a site, as dictated by a "
                "dispatch instance of this script.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
PARSER.add_argument(
    "-r", "--rank",
    required=True,
    type=int,
    help="The Tranco rank of the site at the time the list was generated.")
PARSER.add_argument(
    "-u", "--url",
    required=True,
    help="The URL to record in PageGraph (must be a fully formed URL).")
PARSER.add_argument(
    "-s", "--sec",
    type=int,
    default=10,
    help="Number of seconds let the page execute before requesting the graph.")
PARSER.add_argument(
    "-m", "--max",
    type=int,
    default=90,
    help="The maxim number of seconds we'll wait, after requesting the graph, "
         "before killing the process and reporting an error.")
PARSER.add_argument(
    "--binary", "-b",
    default=str(BINARY_PATH),
    help="The binary to use when calling pagegraph-crawl.")
PARSER.add_argument(
    "--quiet", "-q",
    default=False,
    action="store_true",
    help="Suppress non-error messages and logging.")
PARSER.add_argument(
    "--s3",
    default=pgcrawl.S3_BUCKET,
    help="The S3 bucket to write the resulting graphs into.")

ARGS = PARSER.parse_args()
QUIET = ARGS.quiet

URL = ARGS.url
REQ = CrawlRequest(ARGS.url, ARGS.rank)

RESULT = go(REQ, ARGS.binary, ARGS.sec, ARGS.max, ARGS.s3, QUIET)
if not RESULT:
    sys.exit(1)
else:
    sys.exit(0)
