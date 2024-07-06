#!/usr/bin/env bash

LOCAL_SCRIPTS="client_setup.py dispatch_setup.py client.py dispatch.py pgcrawl/*.py pgcrawl/**/*.py"

pylint $LOCAL_SCRIPTS
mypy --strict $LOCAL_SCRIPTS
