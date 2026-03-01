#!/usr/bin/env python3
import argparse
import os
import signal
import subprocess
import sys


def _sigint(sig, frame):
    sys.exit(0)


signal.signal(signal.SIGINT, _sigint)


def main():
    parser = argparse.ArgumentParser(description='SportSearch CLI')
    parser.add_argument('--index-dir', default='index')
    parser.add_argument('--query')
    parser.add_argument('--input-file')
    parser.add_argument('--output-file')
    args = parser.parse_args()

    qry_bin = 'bin/qry'
    if os.name == 'nt':
        qry_bin = 'bin\\qry.exe'
    if not os.path.exists(qry_bin):
        print(f'Error: search binary not found at {qry_bin}')
        return

    proc = subprocess.Popen(
        [qry_bin, args.index_dir],
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
        print('Error: search engine failed to start')
        return

    interactive = not args.query and not args.input_file and sys.stdin.isatty()

    def run_query(q: str):
        q = q.strip()
        if not q:
            return None, []
        try:
            proc.stdin.write(q + '\n')
            proc.stdin.flush()
            results = []
            header = proc.stdout.readline().strip()
            while True:
                line = proc.stdout.readline()
                if not line or '__END_QUERY__' in line:
                    break
                results.append(line.strip())
            return header, results
        except BrokenPipeError:
            sys.exit(1)

    def show(query, header, results, fout=None):
        if fout:
            fout.write(f'Query: {query}\n')
            if header:
                fout.write(f'{header}\n')
                for i, r in enumerate(results):
                    fout.write(f'{i + 1}. {r}\n')
            else:
                fout.write('No results.\n')
            fout.write('\n')
        elif interactive:
            print(header)
            for r in results:
                print(f' - {r}')
        else:
            print(f'Query: {query}')
            print(header)
            for i, r in enumerate(results):
                print(f'{i + 1}. {r}')
            print('-' * 40)

    try:
        if args.query:
            h, res = run_query(args.query)
            show(args.query, h, res)

        elif args.input_file:
            fout = open(args.output_file, 'w', encoding='utf-8') if args.output_file else None
            try:
                with open(args.input_file, 'r', encoding='utf-8') as fin:
                    for line in fin:
                        q = line.strip()
                        if not q:
                            continue
                        h, res = run_query(q)
                        show(q, h, res, fout)
            finally:
                if fout:
                    fout.close()

        else:
            if interactive:
                print('SportSearch> ', end='', flush=True)
            for line in sys.stdin:
                q = line.strip()
                if q == 'exit':
                    break
                if not q:
                    continue
                h, res = run_query(q)
                show(q, h, res)
                if interactive:
                    print('SportSearch> ', end='', flush=True)

    except KeyboardInterrupt:
        pass
    finally:
        proc.terminate()


if __name__ == '__main__':
    main()
