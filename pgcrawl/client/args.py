from dataclasses import dataclass

import pgcrawl


@dataclass
class ClientCrawlArgs:
    binary_path: str
    # Path to this code (pagegraph-tranco-crawl) on the client machine
    client_code_path: str
    # Name of the s3 bucket the client should write results to
    s3_bucket: str
    # Number of seconds to wait on the page before requesting the pagegraph
    # graph
    pagegraph_secs: int
    # Maximum amount of time to wait for a client to complete a task
    # before assuming something has gone wrong.
    timeout: int


DEFAULT_ARGS = ClientCrawlArgs(
    pgcrawl.DEFAULT_CLIENT_BINARY_PATH,
    pgcrawl.DEFAULT_CLIENT_CODE_PATH,
    pgcrawl.DEFAULT_S3_BUCKET,
    10,
    300)
