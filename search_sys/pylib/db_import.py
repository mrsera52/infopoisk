import argparse
import hashlib
import json
import os
import re
import time
import urllib.parse
from html import unescape
from typing import Optional

import yaml
from pymongo import MongoClient

_WS = re.compile(r'\s+')
_SCRIPT = re.compile(r'(?is)<script[^>]*>.*?</script>')
_STYLE = re.compile(r'(?is)<style[^>]*>.*?</style>')
_TAG = re.compile(r'(?is)<[^>]+>')
_COMMENT = re.compile(r'(?is)<!--.*?-->')


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _yaml(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        d = yaml.safe_load(f)
    return d if isinstance(d, dict) else {}


def _html_to_text(html: str) -> str:
    t = _SCRIPT.sub(' ', html)
    t = _STYLE.sub(' ', t)
    t = _COMMENT.sub(' ', t)
    t = _TAG.sub(' ', t)
    t = unescape(t)
    return _WS.sub(' ', t).strip()


def _connect(cfg: dict):
    db = cfg.get('db') or {}
    conn = db.get('connection_string', 'mongodb://localhost:27017/')
    name = db.get('database', 'sport_search')
    coll_name = db.get('collection', 'articles')
    client = MongoClient(conn)
    coll = client[name][coll_name]
    coll.create_index('source')
    coll.create_index('fetched_at')
    return coll


def import_wiki_dir(coll, data_dir: str) -> int:
    text_dir = os.path.join(data_dir, 'text', 'wikipedia')
    if not os.path.isdir(text_dir):
        print(f'Directory not found: {text_dir}')
        return 0

    added = 0
    skipped = 0
    for fname in os.listdir(text_dir):
        if not fname.endswith('.txt'):
            continue
        stem = fname[:-4]
        title = stem.replace('_', ' ')
        url = f'https://en.wikipedia.org/wiki/{stem}'

        if coll.find_one({'_id': url}):
            skipped += 1
            continue

        fpath = os.path.join(text_dir, fname)
        text = open(fpath, 'r', encoding='utf-8', errors='replace').read()
        raw_sha = _sha(text.encode('utf-8'))
        now = int(time.time())

        doc = {
            '_id': url,
            'url': url,
            'source': 'wikipedia',
            'raw_content': text,
            'raw_sha256': raw_sha,
            'parsed_text': text,
            'fetched_at': now,
            'checked_at': now,
            'title': title,
            'method': 'wikipedia_api',
        }
        coll.insert_one(doc)
        added += 1
        if added % 500 == 0:
            print(f'  imported {added}...')

    print(f'Wikipedia: added={added}, skipped={skipped}')
    return added


def run(config_path: str, data_dir: str, limit: int):
    cfg = _yaml(config_path)
    coll = _connect(cfg)

    print(f'Before import: {coll.count_documents({})} docs')
    import_wiki_dir(coll, data_dir)
    print(f'After import:  {coll.count_documents({})} docs')


def main():
    parser = argparse.ArgumentParser(description='Import articles to MongoDB')
    parser.add_argument('--config', default='conf.yaml')
    parser.add_argument('--data-dir', default='data')
    parser.add_argument('--limit', type=int, default=0)
    args = parser.parse_args()
    run(args.config, args.data_dir, args.limit)


if __name__ == '__main__':
    main()
