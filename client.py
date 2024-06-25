#!/usr/bin/env python3

import argparse
import sys

import pgcrawl
from pgcrawl.client.args import DEFAULT_ARGS, ClientCrawlArgs
from pgcrawl.client.commands import crawl_url


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
    "-s", "--seconds",
    type=int,
    default=DEFAULT_ARGS.pagegraph_secs,
    help="Number of seconds let the page execute before requesting the graph.")
PARSER.add_argument(
    "-t", "--timeout",
    type=int,
    default=DEFAULT_ARGS.timeout,
    help="The maxim number of seconds we'll wait, after requesting the graph, "
         "before killing the process and reporting an error.")
PARSER.add_argument(
    "--binary-path", "-b",
    default=DEFAULT_ARGS.binary_path,
    help="The binary to use when calling pagegraph-crawl.")
PARSER.add_argument(
    "--client-code-path",
    default=DEFAULT_ARGS.client_code_path,
    help="The path to the pagegraph-tranco-crawl library on the client.")
PARSER.add_argument(
    "--s3-bucket",
    default=DEFAULT_ARGS.s3_bucket,
    help="The S3 bucket to write the resulting graphs into.")
PARSER.add_argument(
    "--quiet", "-q",
    default=False,
    action="store_true",
    help="Suppress non-error messages and logging.")

ARGS = PARSER.parse_args()
QUIET = ARGS.quiet

CLIENT_CRAWL_ARGS = ClientCrawlArgs(ARGS.binary_path, ARGS.client_code_path,
                                    ARGS.s3_bucket, ARGS.seconds,
                                    ARGS.timeout)
RESULT = crawl_url(ARGS.url, ARGS.rank, CLIENT_CRAWL_ARGS, QUIET)

if not RESULT:
    sys.exit(1)
else:
    sys.exit(0)
