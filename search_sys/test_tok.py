#!/usr/bin/env python3
import os
import subprocess
import time
from pathlib import Path


def main():
    dirs = [Path('data/text/wikipedia')]
    files = []
    total_bytes = 0
    for d in dirs:
        if d.exists():
            for f in d.glob('*.txt'):
                files.append(f)
                total_bytes += f.stat().st_size

    print('=== Corpus info ===')
    print(f'Files:  {len(files)}')
    print(f'Volume: {total_bytes / 1024 / 1024:.2f} MB')

    sample = files[:1000]
    sample_bytes = sum(f.stat().st_size for f in sample)

    print(f'\n=== Tokenizing sample ({len(sample)} files, {sample_bytes / 1024:.1f} KB) ===')

    texts = [f.read_text(encoding='utf-8', errors='replace') for f in sample]
    combined = '\n'.join(texts)

    tok_bin = 'bin/tok'
    if os.name == 'nt':
        tok_bin = 'bin\\tok.exe'

    t0 = time.time()
    result = subprocess.run(
        [tok_bin], input=combined,
        capture_output=True, text=True,
        encoding='utf-8', errors='replace',
    )
    dt = time.time() - t0

    tokens = [t for t in result.stdout.strip().split('\n')
              if t and t != '__END_DOC__']

    print(f'\n=== Results ===')
    print(f'Tokens:         {len(tokens):,}')
    if tokens:
        avg = sum(len(t) for t in tokens) / len(tokens)
        print(f'Avg length:     {avg:.2f} chars')
    print(f'Time:           {dt:.3f} s')
    if dt > 0:
        print(f'Speed:          {sample_bytes / 1024 / dt:.1f} KB/s')

    unique = set(tokens)
    print(f'Unique tokens:  {len(unique):,}')

    print(f'\n=== First 20 tokens ===')
    for t in tokens[:20]:
        print(f'  {t}')


if __name__ == '__main__':
    main()
