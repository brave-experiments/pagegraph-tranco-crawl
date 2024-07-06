#!/usr/bin/env python3

# Script to populate the work tracking files in work-status, from
# a given tranco file.

import argparse
import csv
import datetime

import pgcrawl
from pgcrawl.dispatch import ALL_DIRS, TODO_DIR
from pgcrawl.setup import mkdirs
from pgcrawl.logging import add_logger_argument, Logger


PARSER = argparse.ArgumentParser(
    prog=f"{pgcrawl.NAME}: dispatch-setup",
    description="Sets up directory structure and other needed steps for the "
                "script running and dispatching the crawl to clients.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
PARSER.add_argument(
    "filename",
    help="Path to a version of a Tranco CSV (see https://tranco-list.eu/)")
PARSER.add_argument(
    "--num", "-n",
    type=int,
    default=15_000,
    help="Number of entries from the given file to write to into the queue.")
add_logger_argument(PARSER)


ARGS = PARSER.parse_args()
LOGGER = Logger(ARGS.log_level)

mkdirs(ALL_DIRS, ARGS.quiet)

with open(ARGS.filename, 'r', encoding="utf8") as csvfile:
    reader = csv.reader(csvfile)
    INDEX = 1
    for (rank, domain) in reader:
        file_name = f"{rank}_{domain}"
        dest_file = TODO_DIR / file_name
        dest_file.write_text(str(datetime.datetime.now()))
        LOGGER.info(f"{INDEX} / {ARGS.num}: Wrote {dest_file}")
        INDEX += 1
        if INDEX > ARGS.num:
            break
