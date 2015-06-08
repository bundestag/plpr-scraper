import os
import requests
from lxml import html
from urlparse import urljoin


DATA_DIR = 'data'
TXT_DIR = os.path.join(DATA_DIR, 'txt')
OUT_DIR = os.path.join(DATA_DIR, 'out')

INDEX_URL = 'https://www.bundestag.de/plenarprotokolle'

for d in TXT_DIR, OUT_DIR:
    try:
        os.makedirs(d)
    except:
        pass


def fetch_protokolle():
    res = requests.get(INDEX_URL)
    doc = html.fromstring(res.content)
    for a in doc.findall('.//a'):
        url = urljoin(INDEX_URL, a.get('href'))
        txt_file = os.path.join(TXT_DIR, os.path.basename(url))
        if not url.endswith('.txt') or os.path.exists(txt_file):
            continue

        with open(txt_file, 'wb') as fh:
            r = requests.get(url)
            fh.write(r.content)

        print url, txt_file


if __name__ == '__main__':
    fetch_protokolle()
