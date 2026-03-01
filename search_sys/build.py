#!/usr/bin/env python3
import os
import subprocess
import sys


def run(cmd, desc):
    print(f'  {desc}...')
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f'  FAILED: {desc}')
        sys.exit(1)


def main():
    print('=== SportSearch Build ===\n')

    print('[1/2] Installing Python packages')
    run(f'{sys.executable} -m pip install -r requirements.txt', 'pip install')

    print('\n[2/2] Compiling C++')
    os.makedirs('bin', exist_ok=True)

    cxx = 'g++'
    flags = '-O2 -std=c++17 -Wall -Wextra'

    targets = [
        ('bin/tok', 'cpp/tok_main.cpp',  'tokenizer'),
        ('bin/idx', 'cpp/idx_main.cpp',  'indexer'),
        ('bin/qry', 'cpp/qry_main.cpp',  'query engine'),
    ]

    for out, src, name in targets:
        if os.name == 'nt':
            out += '.exe'
        run(f'{cxx} {flags} -o {out} {src}', name)

    print('\n=== Build complete ===')


if __name__ == '__main__':
    main()
