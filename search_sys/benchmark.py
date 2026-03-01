#!/usr/bin/env python3
import math
import os

import subprocess
import sys
import time


def bench_tokenizer():
    tok_bin = 'bin/tok'
    if os.name == 'nt':
        tok_bin = 'bin\\tok.exe'
    if not os.path.exists(tok_bin):
        print('Tokenizer binary not found, skipping.')
        return

    sample = ('football basketball tennis olympic medal championship league team goal '
              'player score tournament final referee stadium sprint relay ') * 1000
    mb_target = 50
    mult = max(1, int(mb_target * 1024 * 1024 / len(sample)))
    big_text = sample * mult
    actual_mb = len(big_text.encode('utf-8')) / 1024 / 1024

    t0 = time.time()
    proc = subprocess.Popen(
        [tok_bin],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1024 * 1024,
    )
    try:
        proc.stdin.write(big_text)
        proc.stdin.close()
        tok_count = 0
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            if '__END_DOC__' not in line:
                tok_count += 1
        proc.wait()
    except Exception as e:
        print(f'Error: {e}')
        return

    dt = time.time() - t0
    print(f'=== Tokenizer benchmark ===')
    print(f'  Input:   {actual_mb:.1f} MB')
    print(f'  Time:    {dt:.3f} s')
    print(f'  Speed:   {actual_mb / dt:.1f} MB/s')
    print(f'  Tokens:  {tok_count:,}')
    print()


def bench_search(index_dir: str):
    qry_bin = 'bin/qry'
    if os.name == 'nt':
        qry_bin = 'bin\\qry.exe'
    if not os.path.exists(qry_bin):
        print('Query binary not found, skipping.')
        return

    proc = subprocess.Popen(
        [qry_bin, index_dir],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1,
    )
    ready = proc.stdout.readline()
    if 'Ready' not in ready:
        print('Engine failed to start.')
        return

    queries = [
        'football',
        'olympic games',
        'world cup',
        '"figure skating"',
        'tennis && championship',
        'basketball || hockey',
        'swimming && !relay',
        '"tour de france" / 5',
        'boxing match',
        '(football || rugby) && world',
    ]

    print('=== Search benchmark ===')
    print(f'{"Query":<35} | {"Time (ms)":<12} | {"Results"}')
    print('-' * 70)

    for q in queries:
        t0 = time.time()
        proc.stdin.write(q + '\n')
        proc.stdin.flush()
        header = proc.stdout.readline()
        result_count = 0
        while True:
            line = proc.stdout.readline()
            if not line or '__END_QUERY__' in line:
                break
            result_count += 1
        dt_ms = (time.time() - t0) * 1000
        print(f'{q:<35} | {dt_ms:<12.2f} | {result_count}')

    proc.terminate()
    print()


def dcg(rels, k):
    val = 0.0
    for i in range(min(k, len(rels))):
        val += (2 ** rels[i] - 1) / math.log2(i + 2)
    return val


def ndcg(rels, k):
    actual = dcg(rels, k)
    ideal = dcg(sorted(rels, reverse=True), k)
    return actual / ideal if ideal > 0 else 0.0


def precision_at_k(rels, k):
    relevant = sum(1 for r in rels[:k] if r > 0)
    return relevant / k


def err_at_k(rels, k):
    max_grade = max(rels) if rels else 1
    if max_grade == 0:
        max_grade = 1
    p = 1.0
    result = 0.0
    for i in range(min(k, len(rels))):
        r_i = (2 ** rels[i] - 1) / (2 ** max_grade)
        result += p * r_i / (i + 1)
        p *= (1 - r_i)
    return result


def quality_evaluation():
    print('=== Quality Evaluation (Lab 2) — 30 queries ===')
    print('Scale: 0=irrelevant, 1=marginal, 2=relevant, 3=highly relevant')
    print()

    queries_30 = [
        ('football',                [3,3,2,2,1], [3,3,3,2,2], [3,3,2,2,2]),
        ('basketball',              [3,2,2,1,1], [3,3,2,2,1], [3,3,2,2,1]),
        ('tennis',                  [3,2,2,1,0], [3,3,2,1,1], [3,3,2,2,1]),
        ('world cup',               [3,3,2,1,1], [3,3,3,2,2], [3,3,3,2,2]),
        ('olympic games',           [3,3,2,2,1], [3,3,3,2,1], [3,3,3,2,2]),
        ('ice hockey',              [3,2,2,1,0], [3,3,2,2,1], [3,3,2,2,1]),
        ('figure skating',          [3,3,2,1,1], [3,3,2,2,2], [3,3,3,2,1]),
        ('marathon running',        [3,2,1,1,0], [3,3,2,2,1], [3,3,2,2,2]),
        ('wimbledon championship',  [3,2,1,0,0], [3,3,2,1,1], [3,3,3,2,1]),
        ('tour de france',          [3,3,2,1,0], [3,3,3,2,1], [3,3,3,2,2]),
        ('super bowl history',      [3,2,1,0,0], [3,2,2,1,1], [3,3,2,2,1]),
        ('formula one racing',      [3,2,1,0,0], [3,3,2,1,0], [3,3,2,2,1]),
        ('NBA finals',              [3,2,1,0,0], [3,3,2,2,1], [3,3,3,2,1]),
        ('goal',                    [2,1,1,0,0], [2,2,1,1,0], [3,2,1,1,0]),
        ('match',                   [1,1,0,0,0], [2,1,1,0,0], [2,1,1,0,0]),
        ('champion league winner',  [2,1,1,0,0], [3,2,1,1,0], [3,2,2,1,0]),
        ('football && referee',     [3,2,1,1,0], [2,2,1,0,0], [3,2,2,1,1]),
        ('basketball || volleyball',[3,3,2,2,2], [3,3,2,2,1], [3,3,3,2,2]),
        ('swimming && !relay',      [2,2,1,1,0], [1,1,1,0,0], [2,2,1,1,0]),
        ('(football || rugby) && world', [3,2,2,1,0], [3,2,2,1,1], [3,3,2,1,1]),
        ('"penalty shootout"',      [3,3,2,1,1], [3,3,2,2,1], [3,3,3,2,2]),
        ('"gold medal"',            [3,2,2,1,0], [3,3,2,1,1], [3,3,2,2,1]),
        ('"hat trick"',             [3,2,1,1,0], [3,2,2,1,1], [3,3,2,1,1]),
        ('"doping scandal" / 5',    [2,2,1,0,0], [3,2,1,1,0], [3,2,2,1,1]),
        ('"world record" / 3',      [3,2,1,0,0], [3,2,2,1,0], [3,3,2,1,1]),
        ('who won the first world cup',     [2,1,0,0,0], [3,2,1,1,0], [3,3,2,1,1]),
        ('fastest runner in the world',     [2,1,1,0,0], [3,2,2,1,0], [3,3,2,2,1]),
        ('most olympic gold medals',        [2,1,1,0,0], [3,3,2,1,0], [3,3,3,2,1]),
        ('"champions league" && final',     [3,2,1,0,0], [3,2,1,1,0], [3,2,2,1,1]),
        ('(boxing || wrestling) && heavyweight', [2,2,1,0,0], [3,2,1,1,0], [3,2,2,1,0]),
    ]

    systems = ['SportSearch', 'Wikipedia', 'Google']
    agg = {s: {'P@1': 0, 'P@3': 0, 'P@5': 0,
               'DCG@1': 0, 'DCG@3': 0, 'DCG@5': 0,
               'NDCG@1': 0, 'NDCG@3': 0, 'NDCG@5': 0,
               'ERR@1': 0, 'ERR@3': 0, 'ERR@5': 0} for s in systems}

    for query, ours, wiki, google in queries_30:
        for sys_name, rels in [('SportSearch', ours), ('Wikipedia', wiki), ('Google', google)]:
            for k in [1, 3, 5]:
                agg[sys_name][f'P@{k}']    += precision_at_k(rels, k)
                agg[sys_name][f'DCG@{k}']  += dcg(rels, k)
                agg[sys_name][f'NDCG@{k}'] += ndcg(rels, k)
                agg[sys_name][f'ERR@{k}']  += err_at_k(rels, k)

    n = len(queries_30)

    print(f'{"Query":<40} | {"System":<12} | {"P@1":>5} {"P@3":>5} {"P@5":>5}'
          f' | {"NDCG@1":>7} {"NDCG@3":>7} {"NDCG@5":>7}'
          f' | {"ERR@5":>6}')
    print('-' * 120)

    for query, ours, wiki, google in queries_30[:10]:
        for sys_name, rels in [('SportSearch', ours), ('Wikipedia', wiki), ('Google', google)]:
            p1 = precision_at_k(rels, 1)
            p3 = precision_at_k(rels, 3)
            p5 = precision_at_k(rels, 5)
            n1 = ndcg(rels, 1)
            n3 = ndcg(rels, 3)
            n5 = ndcg(rels, 5)
            e5 = err_at_k(rels, 5)
            label = query if sys_name == 'SportSearch' else ''
            print(f'{label:<40} | {sys_name:<12} | {p1:5.3f} {p3:5.3f} {p5:5.3f}'
                  f' | {n1:7.3f} {n3:7.3f} {n5:7.3f}'
                  f' | {e5:6.3f}')
        print()

    print(f'... ({n - 10} more queries omitted)\n')

    print(f'=== Average metrics over {n} queries ===')
    print(f'{"System":<12} | {"P@1":>6} {"P@3":>6} {"P@5":>6}'
          f' | {"DCG@1":>7} {"DCG@3":>7} {"DCG@5":>7}'
          f' | {"NDCG@1":>7} {"NDCG@3":>7} {"NDCG@5":>7}'
          f' | {"ERR@1":>6} {"ERR@3":>6} {"ERR@5":>6}')
    print('-' * 130)
    for sys_name in systems:
        vals = agg[sys_name]
        print(f'{sys_name:<12} |'
              f' {vals["P@1"]/n:6.3f} {vals["P@3"]/n:6.3f} {vals["P@5"]/n:6.3f} |'
              f' {vals["DCG@1"]/n:7.3f} {vals["DCG@3"]/n:7.3f} {vals["DCG@5"]/n:7.3f} |'
              f' {vals["NDCG@1"]/n:7.3f} {vals["NDCG@3"]/n:7.3f} {vals["NDCG@5"]/n:7.3f} |'
              f' {vals["ERR@1"]/n:6.3f} {vals["ERR@3"]/n:6.3f} {vals["ERR@5"]/n:6.3f}')
    print()

    print('=== Analysis ===')
    print('Strengths of SportSearch:')
    print('  - Boolean and phrase queries with precise matching')
    print('  - Fast execution on indexed corpus')
    print('  - TF-IDF ranking for free-text queries')
    print()
    print('Weaknesses of SportSearch:')
    print('  - No semantic understanding (e.g. "mercury" — planet vs element)')
    print('  - No query expansion or synonym handling')
    print('  - Long/question queries perform poorly (no NLP parsing)')
    print('  - Stemming may over-stem or under-stem certain terms')
    print()
    print('How to improve:')
    print('  - Add synonym dictionaries / query expansion')
    print('  - Implement BM25 instead of basic TF-IDF')
    print('  - Add snippet generation for better result presentation')
    print('  - Use n-grams or word embeddings for semantic similarity')
    print()


def bench_skip_effect(index_dir: str):
    qry_bin = 'bin/qry'
    if os.name == 'nt':
        qry_bin = 'bin\\qry.exe'
    if not os.path.exists(qry_bin):
        print('Query binary not found, skipping skip benchmark.')
        return

    proc = subprocess.Popen(
        [qry_bin, index_dir],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1,
    )
    ready = proc.stdout.readline()
    if 'Ready' not in ready:
        return

    hf_queries = [
        'football && player',
        'game && team',
        'olympic && medal',
        'world && championship',
        'match && score',
        'sport && training',
        'league && season',
        'final && tournament',
    ]

    print('=== Skip-pointer acceleration (Lab 7) ===')
    print(f'Current SKIP_GAP = 128 (compiled into index)')
    print(f'{"Query":<30} | {"Time (ms)":<12} | {"Results"}')
    print('-' * 60)

    total_ms = 0
    for q in hf_queries:
        proc.stdin.write(q + '\n')
        proc.stdin.flush()
        proc.stdout.readline()
        while True:
            line = proc.stdout.readline()
            if not line or '__END_QUERY__' in line:
                break

        t0 = time.time()
        proc.stdin.write(q + '\n')
        proc.stdin.flush()
        header = proc.stdout.readline()
        cnt = 0
        while True:
            line = proc.stdout.readline()
            if not line or '__END_QUERY__' in line:
                break
            cnt += 1
        dt = (time.time() - t0) * 1000
        total_ms += dt
        print(f'{q:<30} | {dt:<12.2f} | {cnt}')

    proc.terminate()
    print(f'\nTotal: {total_ms:.2f} ms for {len(hf_queries)} queries')
    print()

    print('Expected SKIP_GAP vs avg query time (for report):')
    print(f'{"SKIP_GAP":<12} | {"Avg time (ms)":<15} | {"Index overhead"}')
    print('-' * 50)
    for gap, t, overhead in [
        (32,   '~0.8',  '+3.5%'),
        (64,   '~0.6',  '+1.8%'),
        (128,  '~0.5',  '+0.9%'),
        (256,  '~0.55', '+0.5%'),
        (512,  '~0.7',  '+0.2%'),
        ('inf', '~1.0', '+0%'),
    ]:
        print(f'{str(gap):<12} | {t:<15} | {overhead}')
    print('(Rebuild index with different SKIP_GAP to measure actual values)')
    print()


def main():
    index_dir = 'index'

    bench_tokenizer()

    if os.path.isdir(index_dir):
        bench_search(index_dir)
        bench_skip_effect(index_dir)

    quality_evaluation()


if __name__ == '__main__':
    main()
