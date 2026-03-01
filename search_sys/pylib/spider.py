import argparse
import gzip
import hashlib
import os
import re
import time


import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from html import unescape
from typing import Optional, List, Tuple, Dict

import yaml
from pymongo import MongoClient


def _ts() -> int:
    return int(time.time())


def _yaml(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        d = yaml.safe_load(f)
    return d if isinstance(d, dict) else {}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _norm_url(url: str) -> Optional[str]:
    if not isinstance(url, str):
        return None
    url = url.strip()
    if not url:
        return None
    p = urllib.parse.urlsplit(url)
    if p.scheme.lower() not in ('http', 'https'):
        return None
    return urllib.parse.urlunsplit(
        (p.scheme.lower(), p.netloc.lower(), p.path or '/', p.query, ''))


def _strip_qs(url: str) -> str:
    p = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, '', ''))


def _domain_ok(url: str, allowed: List[str]) -> bool:
    try:
        host = urllib.parse.urlsplit(url).netloc.lower().split('@')[-1].split(':')[0]
    except Exception:
        return False
    for d in allowed:
        d = d.lower().strip()
        if host == d or host.endswith('.' + d):
            return True
    return False


_SKIP_EXT = frozenset([
    '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg',
    '.pdf', '.zip', '.rar', '.7z', '.tar', '.gz',
    '.mp3', '.mp4', '.avi', '.mov', '.css', '.js', '.ico',
])


def _html_like(url: str) -> bool:
    try:
        ext = os.path.splitext(urllib.parse.urlsplit(url).path.lower())[1]
    except Exception:
        return True
    return ext not in _SKIP_EXT


_RE_HREF = re.compile(r'(?is)href\s*=\s*(?:"([^"]+)"|\'([^\']+)\'|([^\s"\'<>]+))')
_RE_TITLE = re.compile(r'(?is)<title[^>]*>(.*?)</title>')
_RE_SCRIPT = re.compile(r'(?is)<script[^>]*>.*?</script>')

##28a
_RE_STYLE = re.compile(r'(?is)<style[^>]*>.*?</style>')
_RE_TAG = re.compile(r'(?is)<[^>]+>')
_RE_COMMENT = re.compile(r'(?is)<!--.*?-->')
_RE_WS = re.compile(r'\s+')


def _extract_links(html: str, base: str) -> List[str]:
    out = []
    for m in _RE_HREF.finditer(html):
        href = (m.group(1) or m.group(2) or m.group(3) or '').strip()
        if not href or href.startswith('#') or href.startswith('javascript:'):

            continue
        abs_url = _norm_url(urllib.parse.urljoin(base, href))
        if abs_url:
            out.append(abs_url)
    return out


def _html_to_text(html: str) -> str:
    t = _RE_SCRIPT.sub(' ', html)
    t = _RE_STYLE.sub(' ', t)
    t = _RE_COMMENT.sub(' ', t)
    t = _RE_TAG.sub(' ', t)
    t = unescape(t)
    return _RE_WS.sub(' ', t).strip()


def _html_title(html: str) -> Optional[str]:
    m = _RE_TITLE.search(html)
    if m:
        return _RE_WS.sub(' ', unescape(m.group(1))).strip()
    return None


_HEADERS = {
    'User-Agent': 'SportSearch/1.0 (educational; en-US)',
    'Accept': 'text/html,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'close',
}


def _http_get(url: str, timeout: int, retries: int) -> Tuple[int, str]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                enc = (resp.headers.get('Content-Encoding') or '').lower()
                if 'gzip' in enc:
                    try:
                        body = gzip.decompress(body)
                    except Exception:
                        pass
                charset = resp.headers.get_content_charset() or 'utf-8'
                try:
                    text = body.decode(charset, errors='replace')
                except LookupError:
                    text = body.decode('utf-8', errors='replace')
                return 200, text
        except urllib.error.HTTPError as e:
            last_err = e
            time.sleep(1.0 * attempt)
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(1.0 * attempt)
    if last_err:
        raise last_err
    raise RuntimeError('HTTP request failed')


def open_collection(cfg: dict):
    db_cfg = cfg.get('db') or {}
    conn = db_cfg.get('connection_string', 'mongodb://localhost:27017/')
    db_name = db_cfg.get('database', 'sport_search')
    coll_name = db_cfg.get('collection', 'articles')
    client = MongoClient(conn)
    coll = client[db_name][coll_name]
    coll.create_index('source')
    coll.create_index('fetched_at')
    return coll


def _fresh(doc: Optional[dict], ttl: int, now: int) -> bool:
    if not doc or ttl <= 0:
        return False
    ts = doc.get('fetched_at')
    if not isinstance(ts, int) or ts <= 0:
        return False
    return (now - ts) < ttl


def _save(coll, url, source, raw, sha, text, title, now, extra=None):
    rec = {
        'url': url,
        'source': source,
        'raw_content': raw,
        'raw_sha256': sha,
        'parsed_text': text,
        'fetched_at': now,
        'checked_at': now,
    }
    if title:
        rec['title'] = title
    if extra:
        rec.update(extra)
    coll.update_one({'_id': url}, {'$set': rec}, upsert=True)


def crawl_web(cfg: dict, src: dict, coll) -> None:
    cr = cfg.get('crawler') or {}
    seeds = src.get('seeds') or []
    allowed = src.get('allowed_domains') or []

    ##
    doc_re = re.compile(src.get('doc_url_regex', ''))
    follow_re_str = src.get('follow_url_regex', '')
    follow_re = re.compile(follow_re_str) if follow_re_str else None
    max_docs = int(src.get('max_articles', 0) or 0)


    delay = int(src.get('delay_ms', cr.get('delay_ms', 400)) or 0)
    timeout = int(cr.get('timeout_s', 25) or 25)
    retries = int(cr.get('max_retries', 3) or 3)
    ttl = int(cr.get('cache_ttl_s', 0) or 0)
    name = src.get('name', 'web')

    queue: deque = deque()
    visited: set = set()
    for s in seeds:
        u = _norm_url(str(s))
        if u:
            queue.append(u)

    saved = 0
    while queue:
        if max_docs > 0 and saved >= max_docs:
            break
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)
        if not _domain_ok(url, allowed) or not _html_like(url):
            continue
        is_doc = bool(doc_re.search(url))
        if follow_re and not follow_re.search(url) and not is_doc:
            continue
        try:
            status, html = _http_get(url, timeout, retries)
        except Exception:
            continue
        if status != 200:
            continue
        for link in _extract_links(html, url):
            if link not in visited:
                queue.append(link)
        if is_doc:
            doc_url = _strip_qs(url)
            now = _ts()
            existing = coll.find_one({'_id': doc_url}, {'fetched_at': 1, 'raw_sha256': 1})
            if _fresh(existing, ttl, now):
                continue
            raw_b = html.encode('utf-8', errors='replace')
            sha = _sha(raw_b)
            if existing and existing.get('raw_sha256') == sha:
                coll.update_one({'_id': doc_url}, {'$set': {'checked_at': now}})
            else:
                text = _html_to_text(html)
                title = _html_title(html) or doc_url
                _save(coll, doc_url, name, html, sha, text, title, now, {'method': 'web'})
                saved += 1
        if delay > 0:
            time.sleep(delay / 1000.0)


def run(config_path: str, source_filter: Optional[str] = None):
    cfg = _yaml(config_path)
    coll = open_collection(cfg)
    for src in cfg.get('sources') or []:
        if not isinstance(src, dict):
            continue
        name = src.get('name', '')
        if source_filter and name != source_filter:
            continue
        method = src.get('method', 'web')
        if method == 'web':
            crawl_web(cfg, src, coll)
        elif method == 'wikipedia':
            pass
        else:
            crawl_web(cfg, src, coll)








def main():
    parser = argparse.ArgumentParser(description='SportSearch spider')
    parser.add_argument('config', nargs='?', default='conf.yaml', help='Path to conf.yaml')
    parser.add_argument('--source', default='', help='Source name filter')
    args = parser.parse_args()
    run(args.config, source_filter=args.source.strip() or None)


if __name__ == '__main__':
    main()
