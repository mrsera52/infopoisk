#!/usr/bin/env python3
import os
import subprocess
import time

from flask import Flask, render_template, request

app = Flask(__name__, template_folder='pages')


class Engine:

    def __init__(self):
        self._proc = None
        self._boot()

    def _boot(self):
        qry_bin = 'bin/qry'
        if os.name == 'nt':
            qry_bin = 'bin\\qry.exe'
        if not os.path.exists(qry_bin):
            return
        self._proc = subprocess.Popen(
            [qry_bin, 'index'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
        )
        self._proc.stdout.readline()

    def query(self, text: str):
        if not self._proc or self._proc.poll() is not None:
            self._boot()
        try:
            self._proc.stdin.write(text + '\n')
            self._proc.stdin.flush()

            header = self._proc.stdout.readline()
            if not header or not header.startswith('Found'):
                return [], 0

            try:
                total = int(header.split()[1])
            except (IndexError, ValueError):
                total = 0

            results = []
            while True:
                line = self._proc.stdout.readline()
                if not line or '__END_QUERY__' in line:
                    break
                parts = line.strip().rsplit(' (', 1)
                if len(parts) == 2:
                    title = parts[0]
                    url = parts[1].rstrip(')')
                    if not title:
                        title = url
                    results.append({'title': title, 'url': url})
                else:
                    results.append({'title': line.strip(), 'url': '#'})
            return results, total

        except Exception:
            return [], 0


engine = Engine()


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/search')
def search():
    q = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int)
    if not q:
        return render_template('home.html')

    t0 = time.time()
    results, total = engine.query(q)
    elapsed = round(time.time() - t0, 4)

    per_page = 50
    start = (page - 1) * per_page
    page_results = results[start:start + per_page]
    has_next = start + per_page < len(results)

    return render_template(
        'found.html',
        query=q,
        results=page_results,
        total=total,
        elapsed=elapsed,
        page=page,
        next_page=page + 1 if has_next else None,
    )


if __name__ == '__main__':
    app.run(debug=True, port=5000)
