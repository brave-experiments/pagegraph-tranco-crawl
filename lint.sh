#!/usr/bin/env bash

LOCAL_SCRIPTS="setup-client.py setup-dispatcher.py pgcrawl/*.py"

pycodestyle $LOCAL_SCRIPTS
mypy --strict $LOCAL_SCRIPTS
