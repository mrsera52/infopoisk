#!/usr/bin/env python3
import json
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

OUT_RAW  = Path('data/raw/wikipedia')
OUT_TEXT = Path('data/text/wikipedia')
TARGET         = 31000
MAX_PER_TOPIC  = 1500
PAUSE          = 0.12

API_URL = 'https://en.wikipedia.org/w/api.php'
UA      = 'SportSearch/1.0 (educational project)'

TOPICS = [
    'football',
    'basketball',
    'tennis',
    'athletics',
    'swimming sport',
    'Olympic Games',
    'FIFA World Cup',
    'baseball',
    'ice hockey',
    'volleyball',
    'boxing',
    'martial arts',
    'cycling sport',
    'cricket sport',
    'rugby union',
    'golf sport',
    'Formula One',
    'skiing sport',
    'figure skating',
    'gymnastics',
    'wrestling sport',
    'badminton',
    'table tennis',
    'handball sport',
    'fencing sport',
    'weightlifting',
    'triathlon',
    'archery sport',
    'rowing sport',
    'esports',
    'surfing sport',
    'skateboarding',
    'marathon running',
    'decathlon',
    'NBA basketball',
    'UEFA Champions League',
    'Super Bowl',
    'Tour de France',
    'Wimbledon tennis',
    'Winter Olympics',
]


def api_search(query: str, offset: int = 0):
    params = {
        'action': 'query',
        'list': 'search',
        'srsearch': query,
        'srlimit': 500,
        'sroffset': offset,
        'srwhat': 'text',
        'format': 'json',
    }
    url = f'{API_URL}?{urllib.parse.urlencode(params)}'
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f'  API error: {e}')
        return [], -1
    results = data.get('query', {}).get('search', [])
    cont = data.get('continue', {}).get('sroffset', -1)
    return results, cont


def fetch_text(title: str):
    params = {
        'action': 'query',
        'titles': title,
        'prop': 'extracts|info',
        'explaintext': 1,
        'exsectionformat': 'plain',
        'inprop': 'url',
        'format': 'json',
    }
    url = f'{API_URL}?{urllib.parse.urlencode(params)}'
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f'    fetch error ({title}): {e}')
        return '', ''
    pages = data.get('query', {}).get('pages', {})
    for pid, page in pages.items():
        if pid == '-1':
            return '', ''
        text = page.get('extract', '')
        page_url = page.get('fullurl',
                            f'https://en.wikipedia.org/wiki/{urllib.parse.quote(title)}')
        return text, page_url
    return '', ''


def safe_name(title: str) -> str:
    return re.sub(r'[^\w\s-]', '', title).strip().replace(' ', '_')[:120]


def store(title: str, text: str, url: str, category: str) -> bool:
    if not text or len(text.split()) < 400:
        return False
    name = safe_name(title)
    txt_path = OUT_TEXT / f'{name}.txt'
    raw_path = OUT_RAW / f'{name}.json'
    if txt_path.exists():
        return False

    meta = {
        'title': title,
        'url': url,
        'category': category,
        'word_count': len(text.split()),
    }
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    lines = [
        f'Title: {title}',
        f'URL: {url}',
        'Source: Wikipedia',
        f'Category: {category}',
        '',
        text,
    ]
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    return True


def main():
    OUT_RAW.mkdir(parents=True, exist_ok=True)
    OUT_TEXT.mkdir(parents=True, exist_ok=True)

    seen = {f.stem.lower() for f in OUT_TEXT.glob('*.txt')}
    total = len(seen)
    print(f'Already downloaded: {total}')

    for topic in TOPICS:
        if total >= TARGET:
            break
        print(f'\n=== Searching: {topic} ===')
        offset = 0
        topic_saved = 0

        while total < TARGET and topic_saved < MAX_PER_TOPIC:
            results, next_off = api_search(topic, offset)
            if not results:
                break
            print(f'  Got {len(results)} results')

            for r in results:
                if total >= TARGET or topic_saved >= MAX_PER_TOPIC:
                    break
                title = r.get('title', '')
                if not title or ':' in title:
                    continue
                sn = safe_name(title)
                if sn.lower() in seen:
                    continue

                print(f'    Downloading: {title[:55]}...')
                text, url = fetch_text(title)
                if store(title, text, url, topic):
                    seen.add(sn.lower())
                    topic_saved += 1
                    total += 1
                    wc = len(text.split())
                    print(f'      Saved ({wc} words). Total: {total}')
                else:
                    print(f'      Skipped (too short)')
                time.sleep(PAUSE)

            if next_off < 0:
                break
            offset = next_off

        print(f'  Saved for topic: {topic_saved}')

    print(f'\n=== Total saved: {total} articles ===')


if __name__ == '__main__':
    main()
