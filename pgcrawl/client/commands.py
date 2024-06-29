from pgcrawl.client.actions import UrlRequest, run
from pgcrawl.client.args import ClientCrawlArgs
from pgcrawl.logging import Logger
from pgcrawl.types import Url


def crawl_url(url: Url, rank: int, args: ClientCrawlArgs,
              logger: Logger) -> bool:
    request = UrlRequest(url, rank)
    return run(request, args.binary_path,
               args.pagegraph_secs, args.s3_bucket, args.timeout, logger)
