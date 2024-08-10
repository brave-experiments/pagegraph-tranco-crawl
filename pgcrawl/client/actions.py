from dataclasses import dataclass
from datetime import datetime
import os
import signal
from subprocess import Popen, TimeoutExpired, PIPE
import urllib.parse
from typing import Optional, TYPE_CHECKING

from pgcrawl.client import AWS_COMPLETE_DIR, AWS_ERROR_DIR, AWS_START_DIR
from pgcrawl.client import CRAWLING_COMPLETE_DIR, CRAWLING_ERROR_DIR
from pgcrawl.client import CRAWLING_START_DIR, PAGEGRAPH_CRAWL_DIR
from pgcrawl.client import RECEIVED_DIR, TMP_DIR, ERROR_DIR, COMPLETE_DIR
from pgcrawl.client import PAGEGRAPH_QUERY_ENV_DIR, PAGEGRAPH_QUERY_PROJECT_DIR
from pgcrawl.subprocesses import call_output, call

if TYPE_CHECKING:
    from pathlib import Path
    from pgcrawl.logging import Logger
    from pgcrawl.types import Url


@dataclass
class UrlRequest:
    url: str
    rank: int

    def validate(self) -> bool:
        try:
            return self.domain() is not None
        except ValueError:
            return False

    def domain(self) -> str:
        parts = urllib.parse.urlparse(self.url)
        if parts.netloc == "":
            raise ValueError("Given a URL without a valid domain")
        return parts.netloc

    def file_name(self) -> str:
        return f"{self.rank}_{self.domain()}"

    def graph_name(self) -> str:
        return f"{self.file_name()}.graphml"


def write_log(dir_path: "Path", req: UrlRequest,
              msg: None | str = None) -> None:
    if msg:
        text = msg
    else:
        text = str(datetime.now())
    dest_path = dir_path / req.file_name()
    dest_path.write_text(text)


def crawl(req: UrlRequest, output_path: "Path", binary_path: str,
          pagegraph_time: int, timeout: int, logger: "Logger") -> bool:
    # pylint: disable=consider-using-with,subprocess-popen-preexec-fn
    crawl_args = [
        "npm", "run", "crawl", "--",
        "-b", binary_path,
        "-o", str(output_path.absolute()),
        "-u", req.url,
        "-t", str(pagegraph_time),
    ]

    output_text = ""
    error_text = ""
    is_success = False
    try:
        args_combined = " ".join(crawl_args)
        write_log(CRAWLING_START_DIR, req, args_combined)
        logger.debug(" - " + args_combined)
        rs = Popen(crawl_args, stdout=PIPE, stderr=PIPE,
                   preexec_fn=os.setsid, cwd=PAGEGRAPH_CRAWL_DIR)
        rs.wait(timeout=timeout)
        assert rs.stdout
        assert rs.stderr
        output_text = rs.stdout.read().decode("utf8")
        error_text = rs.stderr.read().decode("utf8")
        is_success = rs.returncode == 0
    except TimeoutExpired:
        output_text = ""
        error_text = "Crawl timed out"
        os.killpg(os.getpgid(rs.pid), signal.SIGTERM)

    logger.debug("crawl results:")
    logger.debug(output_text)

    if not is_success:
        write_log(CRAWLING_ERROR_DIR, req, error_text)
        logger.error(error_text)
        return False

    if not output_path.is_file():
        error_message = f"No file at {str(output_path)}"
        write_log(CRAWLING_ERROR_DIR, req, error_message)
        logger.error(error_message)
        return False

    write_log(CRAWLING_COMPLETE_DIR, req, output_text)
    return True


def get_from_s3(req: UrlRequest, output_path: "Path", s3_bucket: str,
                logger: "Logger") -> bool:
    s3_url = f"s3://{s3_bucket}/{req.graph_name()}"
    aws_args = ["aws", "s3", "cp", "--no-progress", s3_url, str(output_path)]
    return call(aws_args, logger)


def write_to_s3(local_file: "Path", s3_url: "Url", logger: "Logger",
                timeout: int = 30) -> bool:
    aws_args = ["aws", "s3", "mv", "--no-progress", str(local_file), s3_url]
    return call(aws_args, logger, timeout)


def write_crawl_results_to_s3(req: UrlRequest, local_file: "Path",
                              s3_bucket: str, logger: "Logger",
                              timeout: int = 30) -> bool:
    s3_url = f"s3://{s3_bucket}/graphs/{req.graph_name()}"
    write_log(AWS_START_DIR, req,
              f"from: {str(local_file)} -> {s3_url}")
    if write_to_s3(local_file, s3_url, logger, timeout):
        write_log(AWS_COMPLETE_DIR, req)
        return True
    write_log(AWS_ERROR_DIR, req)
    return False


def crawl_and_save(req: UrlRequest, output_path: "Path", binary_path: str,
                   pagegraph_secs: int, s3_bucket: str, timeout: int,
                   logger: "Logger") -> bool:
    logger.debug(f"1. Recording received {req.file_name()}")

    logger.debug(f"2. Starting crawl of {req.url} to {str(output_path)}")
    crawl_rs = crawl(req, output_path, binary_path, pagegraph_secs,
                     timeout, logger)
    if not crawl_rs:
        return False

    logger.debug("3. Writing results to S3")
    return write_crawl_results_to_s3(req, output_path, s3_bucket,
                                     logger, timeout)


def run_crawl(request: UrlRequest, binary_path: str,
              pagegraph_secs: int, s3_bucket: str, timeout: int,
              logger: "Logger") -> bool:
    output_path = TMP_DIR / request.graph_name()
    write_log(RECEIVED_DIR, request)
    rs = crawl_and_save(request, output_path, binary_path,
                        pagegraph_secs, s3_bucket, timeout, logger)

    if output_path.is_file():
        output_path.unlink()

    if not rs:
        write_log(ERROR_DIR, request)
        return False
    write_log(COMPLETE_DIR, request)
    return True


def make_pg_query_cmd(command: str) -> str:
    env_activate_path = PAGEGRAPH_QUERY_ENV_DIR / "bin/activate"
    source_cmd = "source " + str(env_activate_path)

    run_path = PAGEGRAPH_QUERY_PROJECT_DIR / "run.js"
    target_command = f"{str(run_path)} {command}"

    complete_cmd = " && ".join([
        source_cmd,
        target_command
    ])
    return complete_cmd


def run_pg_query_cmd(command: str, logger: "Logger") -> Optional[str]:
    pg_cmd = make_pg_query_cmd(command)
    return call_output(pg_cmd, logger, shell=True)


def run_queries(request: UrlRequest, s3_bucket: str,
                logger: "Logger") -> dict[str, dict[str, str]] | bool:
    output_path = TMP_DIR / request.graph_name()
    graph_path_str = str(output_path)

    try:
        get_from_s3(request, output_path, s3_bucket, logger)

        frames_rs = run_pg_query_cmd(
            f"subframes {graph_path_str} -l | jq -r '.[].\"child frames\".[].id'",
            logger)
        if frames_rs is None:
            return False

        frames_nids = frames_rs.split("\n")
        results = {}
        for nid in frames_nids:
            num_requests = run_pg_query_cmd(
                f"requests -f {nid} {graph_path_str} | jq length", logger)
            if num_requests is None:
                return False

            num_js = run_pg_query_cmd(
                f"js-calls -f {nid} {graph_path_str} | jq '.[].method.name | length'",
                logger)
            if num_js is None:
                return False

            num_html = run_pg_query_cmd(
                f"html -b -s -f {nid} {graph_path_str} | jq '.[] | length'",
                logger)
            if num_html is None:
                return False
            results[nid] = {
                "requests": num_requests,
                "js": num_js,
                "html": num_html
            }
        return results
    finally:
        if output_path.is_file():
            output_path.unlink()
