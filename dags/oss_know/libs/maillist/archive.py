import itertools
import random
import requests
import time

from opensearchpy import OpenSearch

from oss_know.libs.util.log import logger
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.opensearch_api import OpensearchAPI


class MailListArchive:
    def __init__(self, archive_type, rule_type):
        self.archive_type = archive_type
        self.base_uri = ""
        self.rule_generator = generators["rule_type"]
        self.fetcher = fetchers[archive_type]

    def get_urls(self, since=None, until=None):
        return self.rule_generator.generate(since, until)


    def parse(self):
        py_dict = {}
        if self.archive_type == 'gzip':
            # download the gzip
            # extract the gzip to a folder
            # perceval.mbox => python dict
            pass
        elif self.archive_type == 'txt':
            # download the txt to a folder
            # perceval.mbox => python dict
            pass
        elif self.archive_type == 'pipermail':
            # perceval.pipermail => python dict
            pass
        elif self.archive_type == 'hyperkitty':
            # perceval.hyperkitty => python dict
            pass
        else:
            raise(Exception(f'Unexcpected mail list archive type:{self.archive_type}'))
        return py_dict

    def download(self):
        urls = self.get_urls()
        for url in urls:
            archive_path = self.fetcher.download(url)


fetchers = {
    "gzip": "bar"
}

generators = {
    "foo": "bar"
}

defs = {
    "curl": {
        "archive_type": "gzip"
    },
    "rpm": {
        "archive_type": "txt"
    },
    "xproject": {
        "archive_type": "pipermail"
    },
    "yproject": {
        "archive_type": "hyperkitty"
    }
}

from perceval.backends.core.mbox import MBox


class Fetcher:
    def __init__(self, uri):
        self.uri = uri
        pass
    def fetch(self):
        pass


class HttpFetcher(Fetcher):
    # gzip & txt formatted mbox files should be downloaded for
    # perceval.mbox to parse
    def __init__(self, uri, archive_type):
        self.uri = uri
        self.archive_type = archive_type

    def fetch(self, file_dir):
        res = requests.get(self.uri)


def fetch_archive(opensearch_conn_info, project_name, since=None, until=None):
    return "END::init_github_commits"

def parse_archive():
    pass


def store_archive():
    pass
