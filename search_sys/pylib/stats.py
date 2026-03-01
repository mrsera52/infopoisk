import argparse
import collections
import os
import struct
import time
from typing import Counter

import yaml
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pymongo import MongoClient

from .tok_proc import Stemmer

MAGIC_TERM = b'TERM'

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']


def _yaml(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        d = yaml.safe_load(f)
    return d if isinstance(d, dict) else {}


def _get_coll(cfg: dict):
    db = cfg.get('db') or {}
    conn = db.get('connection_string', 'mongodb://localhost:27017/')
    name = db.get('database', 'sport_search')
    return MongoClient(conn)[name][db.get('collection', 'articles')]


def _count_batch(stemmer: Stemmer, texts: list) -> Counter:
    if not texts:
        return collections.Counter()
    combined = ' '.join(texts)
    tokens = stemmer.process(combined)
    return collections.Counter(tokens)


def term_stats_from_index(index_dir: str):
    path = os.path.join(index_dir, 'index.term')
    if not os.path.exists(path):
        return 0, 0
    try:
        with open(path, 'rb') as f:
            magic = f.read(4)
            if magic != MAGIC_TERM:
                return 0, 0
            f.read(2)
            count = struct.unpack('<I', f.read(4))[0]
            total_len = 0
            for _ in range(count):
                tl = struct.unpack('<B', f.read(1))[0]
                f.seek(tl, 1)
                total_len += tl
                f.seek(12, 1)
        return count, total_len
    except Exception:
        return 0, 0


def analyze(config_path: str, tokenizer_path: str, limit: int, output_img: str):
    if not os.path.exists(tokenizer_path):
        raise FileNotFoundError(f'Tokenizer not found: {tokenizer_path}')

    cfg = _yaml(config_path)
    coll = _get_coll(cfg)

    raw_total = 0
    text_total = 0
    doc_count = 0
    tok_count = 0
    tok_len_sum = 0

    cursor = coll.find({'parsed_text': {'$exists': True}})
    if limit > 0:
        cursor = cursor.limit(limit)

    freq = collections.Counter()
    batch_sz = 500
    batch = []

    t0 = time.time()
    stemmer = Stemmer(tokenizer_path)

    try:
        for doc in cursor:
            doc_count += 1
            raw = doc.get('raw_content', '')
            txt = doc.get('parsed_text', '')
            raw_total += len(raw)
            text_total += len(txt)

            if txt:
                batch.append(txt)

            if len(batch) >= batch_sz:
                c = _count_batch(stemmer, batch)
                freq.update(c)
                for token, count in c.items():
                    tok_len_sum += len(token) * count
                    tok_count += count
                batch = []

        if batch:
            c = _count_batch(stemmer, batch)
            freq.update(c)
            for token, count in c.items():
                tok_len_sum += len(token) * count
                tok_count += count
    finally:
        stemmer.shutdown()

    elapsed = time.time() - t0

    idx_terms, idx_term_len = term_stats_from_index('index')

    print('\n' + '=' * 35)
    print('=== Corpus Statistics ===')
    print(f'Documents:        {doc_count}')
    print(f'Raw size:         {raw_total:,} bytes ({raw_total / 1024 / 1024:.2f} MB)')
    print(f'Text size:        {text_total:,} bytes ({text_total / 1024 / 1024:.2f} MB)')
    if doc_count > 0:
        print(f'Avg raw/doc:      {raw_total / doc_count:.0f} bytes')
        print(f'Avg text/doc:     {text_total / doc_count:.0f} bytes')

    print('\n=== Token / Term Stats ===')
    print(f'Total tokens:     {tok_count:,}')
    if tok_count > 0:
        print(f'Avg token length: {tok_len_sum / tok_count:.2f}')
    print(f'Unique terms:     {idx_terms:,}')
    if idx_terms > 0:
        print(f'Avg term length:  {idx_term_len / idx_terms:.2f}')
    if tok_count > 0 and idx_terms > 0:
        diff = tok_len_sum / tok_count - idx_term_len / idx_terms
        print(f'Difference:       {diff:.2f}')
    print(f'Speed:            {text_total / 1024 / elapsed:.1f} KB/s')
    print('=' * 35 + '\n')

    if not freq:
        return

    top10 = freq.most_common(10)
    print('=== Top-10 Terms ===')
    print(f'{"Rank":<6} {"Term":<20} {"Frequency":>12}')
    print('-' * 40)
    for i, (term, count) in enumerate(top10, 1):
        print(f'{i:<6} {term:<20} {count:>12,}')
    print()

    ranked = freq.most_common()
    freqs = [c for _, c in ranked]
    ranks = range(1, len(freqs) + 1)

    plt.figure(figsize=(10, 6))
    plt.loglog(ranks, freqs, marker='.', linestyle='none', markersize=2,
               label='Corpus')
    C = freqs[0]
    zipf = [C / r for r in ranks]
    plt.loglog(ranks, zipf, '--', color='red', linewidth=2, label="Zipf's Law")
    plt.title("Zipf's Law — SportSearch Corpus", fontsize=14)
    plt.xlabel('Rank (log)')
    plt.ylabel('Frequency (log)')
    plt.legend()
    plt.grid(True, which='both', ls='-', alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_img, dpi=150)
    print(f'Plot saved: {output_img}')


def main():
    tok_default = 'bin\\tok.exe' if os.name == 'nt' else 'bin/tok'
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='conf.yaml')
    parser.add_argument('--tokenizer', default=tok_default)
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--output', default='zipf_sport.png')
    args = parser.parse_args()
    analyze(args.config, args.tokenizer, args.limit, args.output)


if __name__ == '__main__':
    main()
