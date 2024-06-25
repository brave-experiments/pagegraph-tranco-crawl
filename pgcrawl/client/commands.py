import pgcrawl.client
from pgcrawl.client.actions import write_log, UrlRequest, run
from pgcrawl.client.args import ClientCrawlArgs
from pgcrawl.logging import log, log_error
from pgcrawl.types import Url


def crawl_url(url: Url, rank: int, args: ClientCrawlArgs, quiet: bool) -> bool:
    request = UrlRequest(url, rank)
    return run(request, args.binary_path,
               args.pagegraph_secs, args.s3_bucket, args.timeout, quiet)
