from pgcrawl.client.actions import UrlRequest, run_crawl, run_queries
from pgcrawl.client.args import ClientCrawlArgs, ClientQueryArgs
from pgcrawl.logging import Logger
from pgcrawl.types import JSONDict, Url


def crawl_url(url: Url, rank: int, args: ClientCrawlArgs,
              logger: Logger) -> bool:
    request = UrlRequest(url, rank)
    return run_crawl(request, args.binary_path,
                     args.pagegraph_secs, args.s3_bucket, args.timeout, logger)


def query_graph(url: Url, rank: int, args: ClientQueryArgs,
                logger: Logger) -> JSONDict | bool:
    request = UrlRequest(url, rank)
    return run_queries(request, args.s3_bucket, logger)
