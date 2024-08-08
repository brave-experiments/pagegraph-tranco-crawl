from dataclasses import dataclass

import pgcrawl


@dataclass
class ClientCrawlArgs:
    client_code_path: str
    binary_path: str
    # Name of the s3 bucket the client should write results to
    s3_bucket: str
    # Number of seconds to wait on the page before requesting the pagegraph
    # graph
    pagegraph_secs: int
    # Maximum amount of time to wait for a client to complete a task
    # before assuming something has gone wrong.
    timeout: int


DEFAULT_CRAWL_ARGS = ClientCrawlArgs(
    pgcrawl.DEFAULT_CLIENT_CODE_PATH,
    pgcrawl.DEFAULT_CLIENT_BINARY_PATH,
    pgcrawl.DEFAULT_S3_BUCKET,
    10,
    300)


@dataclass
class ClientQueryArgs:
    # Name of the s3 bucket the client should write results to
    s3_bucket: str
    # Number of seconds to wait for the query to complete (0 means unlimited)
    timeout: int


DEFAULT_QUERY_ARGS = ClientQueryArgs(
    pgcrawl.DEFAULT_S3_BUCKET,
    300)
