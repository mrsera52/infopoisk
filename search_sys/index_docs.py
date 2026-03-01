#!/usr/bin/env python3
import argparse
import os
import subprocess
import yaml
from pymongo import MongoClient


def _yaml(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        d = yaml.safe_load(f)
    return d if isinstance(d, dict) else {}


def _get_coll(cfg: dict):
    db = cfg.get('db') or {}
    conn = db.get('connection_string', 'mongodb://localhost:27017/')
    name = db.get('database', 'sport_search')
    return MongoClient(conn)[name][db.get('collection', 'articles')]


def main():
    parser = argparse.ArgumentParser(description='Build search index')
    parser.add_argument('--config', default='conf.yaml')
    parser.add_argument('--out-dir', default='index')
    parser.add_argument('--limit', type=int, default=0)
    args = parser.parse_args()

    idx_bin = 'bin/idx'
    if os.name == 'nt':
        idx_bin = 'bin\\idx.exe'
    if not os.path.exists(idx_bin):
        print(f'Error: indexer binary not found at {idx_bin}')
        return

    os.makedirs(args.out_dir, exist_ok=True)

    cfg = _yaml(args.config)
    coll = _get_coll(cfg)

    cursor = coll.find(
        {'parsed_text': {'$exists': True}},
        {'parsed_text': 1, 'url': 1, 'title': 1},
    )
    if args.limit > 0:
        cursor = cursor.limit(args.limit)

    proc = subprocess.Popen(
        [idx_bin, args.out_dir],
        stdin=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1024 * 1024,
    )

    count = 0
    try:
        for doc in cursor:
            url = doc.get('url', '').replace('\t', ' ').replace('\n', ' ')
            title = doc.get('title', '').replace('\t', ' ').replace('\n', ' ')
            text = doc.get('parsed_text', '').replace('\t', ' ').replace('\n', ' ')
            proc.stdin.write(f'{url}\t{title}\t{text}\n')
            count += 1
            if count % 1000 == 0:
                print(f'  fed {count} docs to indexer...')
    except BrokenPipeError:
        pass
    finally:
        proc.stdin.close()
        proc.wait()

    print(f'Done. Indexed {count} documents.')


if __name__ == '__main__':
    main()
