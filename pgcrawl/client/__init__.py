from pathlib import Path


WORKSPACE_DIR = Path("./workspace")
CLIENT_DIR = WORKSPACE_DIR / "client"

RECEIVED_DIR = CLIENT_DIR / "received"
CRAWLING_START_DIR = CLIENT_DIR / "crawling-start"
CRAWLING_ERROR_DIR = CLIENT_DIR / "crawling-error"
CRAWLING_COMPLETE_DIR = CLIENT_DIR / "crawling-complete"
AWS_START_DIR = CLIENT_DIR / "aws-start"
AWS_ERROR_DIR = CLIENT_DIR / "aws-error"
AWS_COMPLETE_DIR = CLIENT_DIR / "aws-complete"
COMPLETE_DIR = CLIENT_DIR / "complete"
ERROR_DIR = CLIENT_DIR / "error"
TMP_DIR = CLIENT_DIR / "tmp"

DIRS_TO_WRITE = [
    WORKSPACE_DIR,
    CLIENT_DIR,
    RECEIVED_DIR,
    CRAWLING_START_DIR,
    CRAWLING_COMPLETE_DIR,
    CRAWLING_ERROR_DIR,
    AWS_START_DIR,
    AWS_ERROR_DIR,
    AWS_COMPLETE_DIR,
    COMPLETE_DIR,
    ERROR_DIR,
    TMP_DIR
]

PAGEGRAPH_CRAWL_DIR = CLIENT_DIR / "pagegraph-crawl"
PAGEGRAPH_QUERY_ENV_DIR = CLIENT_DIR / "pagegraph-query"
PAGEGRAPH_QUERY_PROJECT_DIR = PAGEGRAPH_QUERY_ENV_DIR / "pagegraph-query"
