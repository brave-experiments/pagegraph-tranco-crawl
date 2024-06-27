from dataclasses import dataclass
from datetime import datetime
from io import StringIO
import os
from pathlib import Path
import signal
import subprocess
from subprocess import Popen, TimeoutExpired, PIPE
import urllib.parse

from pgcrawl.client import AWS_COMPLETE_DIR, AWS_ERROR_DIR, AWS_START_DIR
from pgcrawl.client import CRAWLING_COMPLETE_DIR, CRAWLING_ERROR_DIR
from pgcrawl.client import CRAWLING_START_DIR, PAGEGRAPH_CRAWL_DIR
from pgcrawl.client import RECEIVED_DIR, TMP_DIR, ERROR_DIR, COMPLETE_DIR
from pgcrawl.logging import log_error, log


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


def write_log(dir_path: Path, req: UrlRequest,
              msg: None | str = None) -> None:
    if msg:
        text = msg
    else:
        text = str(datetime.now())
    dest_path = dir_path / req.file_name()
    dest_path.write_text(text)


def crawl(req: UrlRequest, output_path: Path, binary_path: str,
          pagegraph_time: int, timeout: int, quiet: bool) -> bool:
    crawl_args = [
        "npm", "run", "crawl", "--",
        "-b", binary_path,
        "-o", str(output_path.absolute()),
        "-u", req.url,
        "-t", str(pagegraph_time),
    ]

    output_text = ""
    error_text = ""
    stderr_stream = StringIO()
    is_success = False
    try:
        args_combined = " ".join(crawl_args)
        write_log(CRAWLING_START_DIR, req, args_combined)
        log(" - " + args_combined, quiet)
        rs = Popen(crawl_args, stdout=PIPE, stderr=PIPE,
                   preexec_fn=os.setsid, cwd=PAGEGRAPH_CRAWL_DIR)
        rs.wait(timeout=timeout)
        assert rs.stdout
        assert rs.stderr
        output_text = rs.stdout.read().decode("utf8")
        error_text = rs.stderr.read().decode("utf8")
        is_success = (rs.returncode == 0)
    except TimeoutExpired:
        output_text = ""
        error_text = "Crawl timed out"
        os.killpg(os.getpgid(rs.pid), signal.SIGTERM)

    log(f"crawl results:", quiet)
    log(output_text, quiet)

    if not is_success:
        write_log(CRAWLING_ERROR_DIR, req, error_text)
        log_error(error_text)
        return False

    if not output_path.is_file():
        error_message = f"No file at {str(output_path)}"
        write_log(CRAWLING_ERROR_DIR, req, error_message)
        log_error(error_message)
        return False

    write_log(CRAWLING_COMPLETE_DIR, req, output_text)
    return True


def write_to_s3(req: UrlRequest, graph_path: Path, s3_bucket: str,
                quiet: bool) -> bool:
    s3_url = f"s3://brave-research-crawling/{req.graph_name()}"
    aws_args = ["aws", "s3", "mv", "--no-progress", str(graph_path), s3_url]

    is_success = False
    stdout_message = ""
    error_message = ""
    try:
        write_log(AWS_START_DIR, req, " ".join(aws_args))
        rs = subprocess.run(aws_args, timeout=30, capture_output=True)
        stdout_message = str(rs.stdout)
        error_message = str(rs.stderr)
        is_success = (rs.returncode == 0)
    except TimeoutExpired:
        error_message = "request to aws timeout"

    if not is_success:
        log_error(error_message)
        write_log(AWS_ERROR_DIR, req, error_message)
        return False

    log(stdout_message, quiet)
    write_log(AWS_COMPLETE_DIR, req, stdout_message)
    return True


def crawl_and_save(req: UrlRequest, output_path: Path, binary_path: str,
                   pagegraph_secs: int, s3_bucket: str, timeout: int,
                   quiet: bool) -> bool:
    log(f"1. Recording received {req.file_name()}", quiet)

    log(f"2. Starting crawl of {req.url} to {str(output_path)}", quiet)
    crawl_rs = crawl(req, output_path, binary_path, pagegraph_secs,
                     timeout, quiet)
    if not crawl_rs:
        return False

    log(f"3. Writing results to S3")
    s3_rs = write_to_s3(req, output_path, s3_bucket, quiet)
    if not s3_rs:
        return False

    return True


def run(request: UrlRequest, binary_path: str,
        pagegraph_secs: int, s3_bucket: str, timeout: int,
        quiet: bool) -> bool:
    output_path = TMP_DIR / request.graph_name()
    write_log(RECEIVED_DIR, request)
    rs = crawl_and_save(request, output_path, binary_path,
                        pagegraph_secs, s3_bucket, timeout, quiet)

    if output_path.is_file():
        output_path.unlink()

    if not rs:
        write_log(ERROR_DIR, request)
        return False
    else:
        write_log(COMPLETE_DIR, request)
        return True
