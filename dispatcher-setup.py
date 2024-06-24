#!/usr/bin/env python3

# Script to populate the work tracking files in work-status, from
# a given tranco file.

import argparse
import csv
import datetime
import pathlib

import pgcrawl
from pgcrawl.setup import mkdirs
from pgcrawl.logging import log


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
PARSER.add_argument(
    "--quiet", "-q",
    default=False,
    action="store_true",
    help="Suppress non-error messages and logging.")

ARGS = PARSER.parse_args()

WORKSPACE_DIR = pathlib.Path("./workspace")
DISPATCHER_DIR = WORKSPACE_DIR / "dispatcher"
DONE_DIR = DISPATCHER_DIR / "done"
TODO_DIR = DISPATCHER_DIR / "todo"
UNDERWAY_DIR = DISPATCHER_DIR / "underway"

ALL_DIRS_TO_CHECK = [
    WORKSPACE_DIR, DISPATCHER_DIR, DONE_DIR, TODO_DIR, UNDERWAY_DIR]

mkdirs(ALL_DIRS_TO_CHECK, ARGS.quiet)

with open(ARGS.filename, 'r') as csvfile:
    reader = csv.reader(csvfile)
    index = 1
    for (rank, domain) in reader:
        file_name = f"{rank}_{domain}"
        dest_file = TODO_DIR / file_name
        dest_file.write_text(str(datetime.datetime.now()))
        log(f"{index} / {ARGS.num}: Wrote {dest_file}")
        index += 1
        if index > ARGS.num:
            break
