#!/usr/bin/env bash

LOCAL_SCRIPTS="client-setup.py dispatcher-setup.py dispatch.py pgcrawl/*.py"

pycodestyle $LOCAL_SCRIPTS
mypy --strict $LOCAL_SCRIPTS
