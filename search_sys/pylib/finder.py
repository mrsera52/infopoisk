import os
import re
import struct
import time
from typing import List, Dict, Set, Tuple, Optional

from . import varint
from .tok_proc import Stemmer

MAGIC_FWD = b'FWRD'
MAGIC_TERM = b'TERM'
MAGIC_DATA = b'DATA'


class IndexReader:

    def __init__(self, index_dir: str):
        self.index_dir = index_dir
        self.documents: List[Dict[str, str]] = []
        self.vocabulary: Dict[str, tuple] = {}

        self._read_forward()
        self._read_vocabulary()

        self._data_fh = open(os.path.join(index_dir, 'index.data'), 'rb')

    def _read_forward(self):
        path = os.path.join(self.index_dir, 'index.fwd')
        with open(path, 'rb') as f:
            magic = f.read(4)
            if magic != MAGIC_FWD:
                raise ValueError('Bad FWD file')
            ver = struct.unpack('<H', f.read(2))[0]
            n = struct.unpack('<I', f.read(4))[0]

            offsets = [struct.unpack('<Q', f.read(8))[0] for _ in range(n)]

            for off in offsets:
                f.seek(off)
                ul = struct.unpack('<H', f.read(2))[0]
                url = f.read(ul).decode('utf-8')
                tl = struct.unpack('<H', f.read(2))[0]
                title = f.read(tl).decode('utf-8')
                dl = struct.unpack('<I', f.read(4))[0]
                self.documents.append({'url': url, 'title': title, 'length': dl})

    def _read_vocabulary(self):
        path = os.path.join(self.index_dir, 'index.term')
        with open(path, 'rb') as f:
            magic = f.read(4)
            if magic != MAGIC_TERM:
                raise ValueError('Bad TERM file')
            struct.unpack('<H', f.read(2))
            cnt = struct.unpack('<I', f.read(4))[0]

            for _ in range(cnt):
                tl = struct.unpack('<B', f.read(1))[0]
                term = f.read(tl).decode('utf-8')
                offset = struct.unpack('<Q', f.read(8))[0]
                df = struct.unpack('<I', f.read(4))[0]
                self.vocabulary[term] = (offset, df)

    def postings(self, term: str) -> Dict[int, List[int]]:
        if term not in self.vocabulary:
            return {}

        offset, expected_df = self.vocabulary[term]
        self._data_fh.seek(offset)

        chunk = self._data_fh.read(1024 * 1024)
        if not chunk:
            return {}

        ptr = 0
        df, ptr = varint.unpack(chunk, ptr)

        num_skips, ptr = varint.unpack(chunk, ptr)
        for _ in range(num_skips):
            _, ptr = varint.unpack(chunk, ptr)
            _, ptr = varint.unpack(chunk, ptr)

        result = {}
        cur_doc = 0
        for _ in range(df):
            if ptr >= len(chunk) - 16:
                extra = self._data_fh.read(1024 * 1024)
                if extra:
                    chunk = chunk[ptr:] + extra
                    ptr = 0

            delta, ptr = varint.unpack(chunk, ptr)
            cur_doc += delta

            freq, ptr = varint.unpack(chunk, ptr)
            positions = []
            cur_pos = 0
            for _ in range(freq):
                if ptr >= len(chunk) - 8:
                    extra = self._data_fh.read(1024 * 1024)
                    if extra:
                        chunk = chunk[ptr:] + extra
                        ptr = 0
                pd, ptr = varint.unpack(chunk, ptr)
                cur_pos += pd
                positions.append(cur_pos)

            result[cur_doc] = positions

        return result

    def doc_info(self, doc_id: int) -> Dict[str, str]:
        if 0 <= doc_id < len(self.documents):
            return self.documents[doc_id]
        return {'url': '', 'title': '', 'length': 0}

    def close(self):
        self._data_fh.close()


class QueryEngine:

    def __init__(self, reader: IndexReader, stemmer: Stemmer):
        self.reader = reader
        self.stemmer = stemmer
        self._all = set(range(len(reader.documents)))

    def search(self, query: str) -> Set[int]:
        tokens = self._lex(query)
        return self._eval(tokens)

    def _lex(self, q: str) -> List[str]:
        q = q.replace('\u00ab', '"').replace('\u00bb', '"')
        pattern = re.compile(
            r'"([^"]+)"|(\d+)|(&&|\|\||!|\(|\)|/)|([^\s"&|!()/]+)')
        out = []
        for m in pattern.finditer(q):
            phrase, num, op, word = m.groups()
            if phrase:
                out.append(f'PH:{phrase}')
            elif num:
                out.append(f'N:{num}')
            elif op:
                out.append(op)
            elif word:
                out.append(word)
        return out

    def _eval(self, tokens: List[str]) -> Set[int]:
        processed = []
        i = 0
        while i < len(tokens):
            t = tokens[i]
            if t.startswith('PH:') or not (
                    t in ('&&', '||', '!', '(', ')', '/') or t.startswith('N:')):
                if (i + 2 < len(tokens)
                        and tokens[i + 1] == '/'
                        and tokens[i + 2].startswith('N:')):
                    dist = int(tokens[i + 2].split(':')[1])
                    content = t[3:] if t.startswith('PH:') else t
                    processed.append(('PROX', content, dist))
                    i += 3
                    continue
                if t.startswith('PH:'):
                    processed.append(('PH', t[3:]))
                else:
                    processed.append(('W', t))
            elif t in ('&&', '||', '!', '(', ')'):
                processed.append(('OP', t))
            i += 1

        final = []
        for idx, tok in enumerate(processed):
            final.append(tok)
            if idx < len(processed) - 1:
                ct, cv = tok[:2]
                nt, nv = processed[idx + 1][:2]
                need_and = (ct != 'OP' or cv == ')') and (nt != 'OP' or nv in ('(', '!'))
                if need_and:
                    final.append(('OP', '&&'))

        prec = {'(': 0, '||': 1, '&&': 2, '!': 3}
        rpn = []
        ops = []
        for tok in final:
            tp = tok[0]
            if tp in ('W', 'PH', 'PROX'):
                rpn.append(tok)
            elif tp == 'OP':
                v = tok[1]
                if v == '(':
                    ops.append(v)
                elif v == ')':
                    while ops and ops[-1] != '(':
                        rpn.append(('OP', ops.pop()))
                    if ops:
                        ops.pop()
                else:
                    while ops and ops[-1] != '(' and prec.get(ops[-1], 0) >= prec.get(v, 0):
                        rpn.append(('OP', ops.pop()))
                    ops.append(v)
        while ops:
            rpn.append(('OP', ops.pop()))

        return self._rpn(rpn)

    def _rpn(self, rpn) -> Set[int]:
        stack = []
        for tok in rpn:
            tp = tok[0]
            if tp == 'W':
                stems = self.stemmer.process(tok[1])
                if stems:
                    p = self.reader.postings(stems[0])
                    stack.append(set(p.keys()))
                else:
                    stack.append(set())
            elif tp == 'PH':
                terms = self.stemmer.process(tok[1])
                stack.append(self._seq_search(terms, len(terms)))
            elif tp == 'PROX':
                terms = self.stemmer.process(tok[1])
                dist = tok[2]
                stack.append(self._seq_search(terms, dist))
            elif tp == 'OP':
                v = tok[1]
                if v == '!' and stack:
                    stack.append(self._all - stack.pop())
                elif v == '&&' and len(stack) >= 2:
                    b, a = stack.pop(), stack.pop()
                    stack.append(a & b)
                elif v == '||' and len(stack) >= 2:
                    b, a = stack.pop(), stack.pop()
                    stack.append(a | b)
        return stack[0] if stack else set()

    def _seq_search(self, terms: List[str], max_dist: int) -> Set[int]:
        if not terms:
            return set()
        all_postings = [self.reader.postings(t) for t in terms]
        if any(not p for p in all_postings):
            return set()
        common = set(all_postings[0].keys())
        for p in all_postings[1:]:
            common &= set(p.keys())

        exact = (max_dist == len(terms))
        hits = set()
        for doc in common:
            pos_lists = [all_postings[i][doc] for i in range(len(terms))]
            if self._match_seq(pos_lists, 0, -1, -1, max_dist, exact):
                hits.add(doc)
        return hits

    def _match_seq(self, pos_lists, idx, prev, first, max_d, exact) -> bool:
        if idx == len(pos_lists):
            return True
        for p in pos_lists[idx]:
            if idx == 0:
                if self._match_seq(pos_lists, 1, p, p, max_d, exact):
                    return True
            elif p > prev:
                if exact and p != prev + 1:
                    continue
                if p - first > max_d:
                    continue
                if self._match_seq(pos_lists, idx + 1, p, first, max_d, exact):
                    return True
        return False


def cli_search():
    import argparse

    parser = argparse.ArgumentParser(description='SportSearch CLI')
    parser.add_argument('--index-dir', default='index')
    parser.add_argument('--tokenizer', default='bin/tok')
    parser.add_argument('--query')
    parser.add_argument('--input-file')
    parser.add_argument('--output-file')
    args = parser.parse_args()

    stemmer = Stemmer(args.tokenizer)
    try:
        reader = IndexReader(args.index_dir)
        engine = QueryEngine(reader, stemmer)

        if args.input_file and args.output_file:
            with (open(args.input_file, 'r', encoding='utf-8') as fin,
                  open(args.output_file, 'w', encoding='utf-8') as fout):
                for line in fin:
                    q = line.strip()
                    if not q:
                        continue
                    fout.write(f'Query: {q}\n')
                    try:
                        results = engine.search(q)
                        fout.write(f'Found: {len(results)} docs\n')
                        for i, did in enumerate(sorted(results)[:10]):
                            info = reader.doc_info(did)
                            fout.write(f'{i + 1}. {info["title"]} ({info["url"]})\n')
                    except Exception as e:
                        fout.write(f'Error: {e}\n')
                    fout.write('\n')

        elif args.query:
            t0 = time.time()
            results = engine.search(args.query)
            dt = time.time() - t0
            print(f'Found {len(results)} docs in {dt:.4f}s')
            for did in sorted(results)[:10]:
                info = reader.doc_info(did)
                print(f' - {info["title"]} ({info["url"]})')

        else:
            while True:
                try:
                    q = input('> ')
                    if q.strip() == 'exit':
                        break
                    t0 = time.time()
                    results = engine.search(q)
                    dt = time.time() - t0
                    print(f'Found {len(results)} docs in {dt:.4f}s')
                    for did in sorted(results)[:10]:
                        info = reader.doc_info(did)
                        print(f' - {info["title"]} ({info["url"]})')
                except (KeyboardInterrupt, EOFError):
                    break
                except Exception as e:
                    print(f'Error: {e}')

        reader.close()
    finally:
        stemmer.shutdown()


if __name__ == '__main__':
    cli_search()
