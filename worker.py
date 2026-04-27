"""GitHub Actions用: HPから運営会社を取得（Google検索なし・rate limit回避）"""
import csv, os, re, sys, time, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from urllib.parse import urlparse

WORKERS = 10
HEADERS_HTTP = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html", "Accept-Language": "ja",
}

COMPANY_PATTERNS = [
    r'((?:株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|NPO法人)\s*[^\s<,、。「」（）\(\)]{2,25})',
    r'([^\s<,、。「」（）\(\)]{2,20}(?:株式会社|有限会社|合同会社|合資会社|合名会社))',
]

EXCLUDE = ['グーグル', 'Google', 'Apple', 'Amazon', 'Microsoft', 'Facebook', 'Meta',
           'Instagram', 'Twitter', 'LINE', 'Yahoo', 'PayPay', 'Uber',
           '食べログ', 'ぐるなび', 'ホットペッパー', 'Retty', '一休',
           'クレジットカード', '決済', '予約', 'システム', 'サービス', 'ブラウザ',
           'WordPress', 'Wix', 'Shopify', 'Square']


def is_valid(name):
    if not name or len(name) < 4: return False
    return not any(ex in name for ex in EXCLUDE)


def extract_company(html):
    companies = []
    for pat in COMPANY_PATTERNS:
        companies.extend(re.findall(pat, html))
    valid = [c.strip() for c in companies if is_valid(c.strip())]
    if valid: return Counter(valid).most_common(1)[0][0]
    return ""


def fetch_company_hp(hp_url):
    if not hp_url: return ""
    try:
        resp = requests.get(hp_url, headers=HEADERS_HTTP, timeout=10, allow_redirects=True)
        if resp.status_code != 200: return ""
        html = resp.text

        company = extract_company(html)
        if company: return company

        parsed = urlparse(hp_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        sub_links = set()
        for pat in [r'href="(/[^"]*(?:company|about|corporate|info|tokusho|legal|gaiyou|outline)[^"]*)"',
                    r'href="(/[^"]*(?:会社概要|運営会社|特定商取引|会社情報|企業情報)[^"]*)"',
                    r'href="(https?://[^"]*(?:company|about|corporate)[^"]*)"']:
            sub_links.update(re.findall(pat, html, re.IGNORECASE))

        for sub in list(sub_links)[:3]:
            try:
                sub_url = base + sub if sub.startswith('/') else sub
                if not sub_url.startswith('http'): continue
                resp2 = requests.get(sub_url, headers=HEADERS_HTTP, timeout=8, allow_redirects=True)
                if resp2.status_code == 200:
                    company = extract_company(resp2.text)
                    if company: return company
            except: continue

        footer = re.search(r'(?:<footer|class="footer|id="footer)([\s\S]{0,2000}?)(?:</footer|$)', html, re.IGNORECASE)
        if footer:
            company = extract_company(footer.group(1))
            if company: return company
        copy = re.search(r'(?:copyright|©|&copy;)([\s\S]{0,300})', html, re.IGNORECASE)
        if copy:
            company = extract_company(copy.group(1))
            if company: return company
    except: pass
    return ""


def fetch_company_tabelog(tabelog_url):
    """食べログから運営会社（rate limitに注意）"""
    if not tabelog_url: return ""
    try:
        resp = requests.get(tabelog_url, headers=HEADERS_HTTP, timeout=15)
        if resp.status_code == 429:
            time.sleep(60)
            return ""  # リトライせず諦める
        if resp.status_code != 200: return ""
        html = resp.text
        for pat in [
            r'運営会社[\s\S]{0,200}?<td[^>]*>\s*([^<]+)',
            r'運営元[\s\S]{0,200}?<td[^>]*>\s*([^<]+)',
        ]:
            m = re.search(pat, html)
            if m and is_valid(m.group(1).strip()):
                return m.group(1).strip()
        company = extract_company(html)
        if company: return company
    except: pass
    return ""


def process_row(row):
    source_file, row_idx, name, pref, tabelog_url, hp_url = row
    # 1. HP（rate limitなし）
    company = fetch_company_hp(hp_url)
    if company: return (*row, company, "HP")
    # 2. 食べログ（控えめに）
    company = fetch_company_tabelog(tabelog_url)
    if company: return (*row, company, "tabelog")
    time.sleep(0.5)
    return (*row, "", "")


def main():
    chunk_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    input_file = f"data/targets_{chunk_id}.csv"
    output_file = f"results/results_{chunk_id}.csv"

    os.makedirs("results", exist_ok=True)

    done_urls = set()
    if os.path.exists(output_file):
        with open(output_file, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            for r in reader:
                if len(r) > 4: done_urls.add(r[4])

    with open(input_file, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        all_rows = [r for r in reader if r[4] not in done_urls]

    print(f"Chunk {chunk_id}: {len(all_rows):,}件（スキップ: {len(done_urls):,}件）", flush=True)

    out_header = header + ["company", "source"]
    if not os.path.exists(output_file):
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(out_header)

    done = 0
    found = 0
    batch_size = 100

    for bs in range(0, len(all_rows), batch_size):
        batch = all_rows[bs:bs+batch_size]
        results = []
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(process_row, row): row for row in batch}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    done += 1
                    if result[-2]: found += 1
                except: pass

        with open(output_file, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for r in results:
                w.writerow(r)

        if done % 500 < batch_size:
            print(f"  {done:,}/{len(all_rows):,} (発見{found:,})", flush=True)

    print(f"\nChunk {chunk_id} 完了: {done:,}件処理, {found:,}件発見", flush=True)


if __name__ == "__main__":
    main()
