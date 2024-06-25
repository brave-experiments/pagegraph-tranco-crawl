#!/usr/bin/env bash

LOCAL_SCRIPTS="client-setup.py dispatch-setup.py client.py dispatch.py pgcrawl/*.py pgcrawl/**/*.py"

pycodestyle $LOCAL_SCRIPTS
mypy --strict $LOCAL_SCRIPTS
