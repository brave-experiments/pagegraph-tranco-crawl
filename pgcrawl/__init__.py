from ipaddress import IPv4Address, IPv6Address
import pathlib

NAME = "pagegraph-tranco-crawl"
GIT_URL = "git@github.com:brave-experiments/pagegraph-tranco-crawl.git"
PG_CRAWL_GIT_URL = "https://github.com/brave/pagegraph-crawl"
BRAVE_INSTALL_SCRIPT = pathlib.Path("./scripts/install-brave-nightly.sh")
DEFAULT_CLIENT_CODE_PATH = "~/pagegraph-tranco-crawl"
S3_BUCKET = "brave-research-crawling"

IPAddress = IPv4Address | IPv6Address
