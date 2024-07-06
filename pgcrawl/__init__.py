from pathlib import Path


DEFAULT_CLIENT_BINARY_PATH = (
    "/opt/brave.com/brave-nightly/brave-browser-nightly")
DEFAULT_CLIENT_CODE_PATH = "/home/ubuntu/pagegraph-tranco-crawl"
DEFAULT_S3_BUCKET = "brave-research-crawling"

NAME = "pagegraph-tranco-crawl"
GIT_URL = "git@github.com:brave-experiments/pagegraph-tranco-crawl.git"
PG_CRAWL_GIT_URL = "https://github.com/brave/pagegraph-crawl"
BRAVE_INSTALL_SCRIPT = Path("./scripts/install-brave-nightly.sh")

PG_QUERY_GIT_URL = "https://github.com/brave-experiments/pagegraph-query.git"
