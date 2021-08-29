import logging
import re
import time
import random

import click
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.error import URLError
from bs4 import BeautifulSoup


@click.command()
def fetch_and_print():
    pass


if __name__ == '__main__':
    fetch_and_print()
