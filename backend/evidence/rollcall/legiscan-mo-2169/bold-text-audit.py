"""Missouri bold-text audit, second version (after review of PR #1234).

Missouri marks NEW matter in bold and DELETED matter in [brackets]. pdftotext keeps
the brackets and loses the bold, so:
  * every [bracketed] span in a bill section is a DELETION — reported directly, no
    statute needed, and it works for enacted bills too;
  * new matter is whatever remains in the bill section after the bracketed spans are
    removed and does NOT appear in the pre-bill statute. That needs the current
    section from revisor.mo.gov, which is only the pre-bill text when the bill did
    NOT become law. For an enacted bill the ADDED list is empty by construction.

Fixes from the review: margin line numbers are stripped per line before the text is
flattened, so numbers inside provisions ("within 30 days", "$500") survive; a section
is cut at the next section heading, not at 20,000 characters; the diff is word-level
with no length floor, so `[ten] twenty` and a one-clause deletion are reported.

Running it needs three things outside this repository:
  * the LegiScan bulk dataset for Missouri session 2169, unzipped, so that
    <dataset dir>/MO/<session>/bill/HB952.json exists
      — set LEGISCAN_MO_DATASET   (default ~/legiscan-data/mo-2169)
  * a writable cache directory for bill PDFs, their text, and statute text
      — set MO_AUDIT_CACHE         (default ~/legiscan-data/mo-work)
  * `curl` and `pdftotext` (poppler) on PATH, and network access to
    documents.house.mo.gov / senate.mo.gov and revisor.mo.gov.

    python3 bold-text-audit.py HB565 2025-04-10
"""
import sys, re, os, glob, json, subprocess, difflib, html

DATASET = os.path.expanduser(os.environ.get('LEGISCAN_MO_DATASET', '~/legiscan-data/mo-2169'))
WORK = os.path.expanduser(os.environ.get('MO_AUDIT_CACHE', '~/legiscan-data/mo-work'))
CACHE = os.path.join(WORK, 'rsmo')
DOCS = os.path.join(WORK, 'docs')
for d in (CACHE, DOCS):
    os.makedirs(d, exist_ok=True)

# ---- the slice of the session's bill records this audit needs (was mo.py) ----
BILLS = {}
for _p in glob.glob(os.path.join(DATASET, 'MO', '*', 'bill', '*.json')):
    _d = json.load(open(_p))['bill']
    BILLS[_d['bill_number']] = _d
if not BILLS:
    sys.exit(f'no bill records under {DATASET}/MO/*/bill/ — set LEGISCAN_MO_DATASET to the unzipped '
             f'LegiScan Missouri 2169 dataset')

def norm(b):
    m = re.match(r'([A-Z]+)0*(\d+)$', b.replace(' ', '').upper())
    return f"{m.group(1)}{int(m.group(2))}"

def _fetch_text(url, name):
    pdf = os.path.join(DOCS, name + '.pdf'); txt = os.path.join(DOCS, name + '.txt')
    if os.path.exists(txt) and os.path.getsize(txt) > 200:
        return open(txt, encoding='utf-8', errors='replace').read()
    if not os.path.exists(pdf) or os.path.getsize(pdf) < 1000:
        r = subprocess.run(['curl', '-sSL', '-o', pdf, '-w', '%{http_code}', url], capture_output=True, text=True)
        if r.stdout.strip() != '200':
            raise RuntimeError(f'{name}: HTTP {r.stdout.strip()} {url}')
    subprocess.run(['pdftotext', '-layout', pdf, txt], check=True, capture_output=True)
    return open(txt, encoding='utf-8', errors='replace').read()

def voted_text(bill, date):
    """The last text printed on or before the vote date — the perfected or engrossed print."""
    b = BILLS[norm(bill)]
    ts = [t for t in b['texts'] if t['date'] <= date] or b['texts']
    return ts[-1]

def docs(bill, date):
    t = voted_text(bill, date)
    return t, _fetch_text(t['state_link'], f"{norm(bill)}-{t['type'].replace(' ', '')}-{t['doc_id']}")

os.makedirs(CACHE, exist_ok=True)
SEC = r'\d+[A-Z]?\.\d+'

def strip_layout(raw):
    """Drop page footers/headers and margin line numbers line by line, then flatten."""
    out = []
    skipping = False
    for line in raw.splitlines():
        s = line.strip()
        if s.startswith('EXPLANATION') and 'bold-faced' in s:
            skipping = True
        if skipping:
            if 'proposed language' in s:
                skipping = False
            continue
        if re.fullmatch(r'(?:HCS|SCS|SS|HB|SB|HJR|SJR|#\d|\s|\d|&|,)+', s) and re.search(r'(HB|SB|HJR|SJR)', s):
            continue                                  # page header: "HCS HB 565 2"
        s = re.sub(r'^\d{1,3}\s+(?=\S)', '', s)      # margin line number only
        if s:
            out.append(s)
    t = ' '.join(out)
    t = re.sub(r'\s+', ' ', t)
    t = re.sub(r'\s+([,;:.])', r'\1', t)             # `activity" ,` -> `activity",`
    return t

def bill_sections(bill, date):
    """{section: body} for every section the bill sets out, cut at the next heading.

    Headings come from the bill's own Section A list, so a section number cited
    inside another section's text is never mistaken for a heading. For each listed
    section the occurrence followed by "1." or "(1)" is the heading; if there is
    none (a section with no subsections), the first occurrence is.
    """
    _, raw = docs(bill, date)
    t = strip_layout(raw)
    m = re.search(r'Section A\.(.*?)to read as follows', t, flags=re.S)
    listed = []
    for sec in re.findall(SEC, m.group(1) if m else ''):
        if sec not in listed:
            listed.append(sec)
    heads = []
    for sec in listed:
        occ = [mm.end() for mm in re.finditer(r'(?:^|\s)' + re.escape(sec) + r'\.\s+', t)]
        if m:
            occ = [o for o in occ if o > m.end()]          # skip the Section A list itself
        if not occ:
            continue
        sub = [o for o in occ if re.match(r'(?:1\.|\(1\))\s', t[o:o + 4])]
        heads.append(((sub or occ)[0], sec))
    heads.sort()
    secs = {}
    for k, (pos, sec) in enumerate(heads):
        end = heads[k + 1][0] if k + 1 < len(heads) else len(t)
        secs[sec] = t[pos:end].strip()
    return secs

def statute(sec):
    p = os.path.join(CACHE, sec + '.v2.txt')
    if os.path.exists(p) and os.path.getsize(p) > 100:
        return open(p, encoding='utf-8').read()
    r = subprocess.run(['curl', '-sSL', f'https://revisor.mo.gov/main/OneSection.aspx?section={sec}'],
                       capture_output=True, text=True)
    t = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', r.stdout, flags=re.S | re.I)
    t = html.unescape(re.sub(r'<[^>]+>', ' ', t))
    t = re.sub(r'\s+', ' ', t)
    m = re.search(re.escape(sec) + r'\.\s+[^—]{0,200}?—\s*', t)   # "537.325. Definitions — ... —"
    body = t[m.end():] if m else t
    body = re.split(r'\(L\. (?:19|20)\d\d|Effective -|\(RSMo ', body)[0]
    body = re.sub(r'\s+([,;:.])', r'\1', body).strip()
    open(p, 'w', encoding='utf-8').write(body)
    return body

def deletions(body):
    return [d.strip() for d in re.findall(r'\[([^\[\]]+)\]', body) if d.strip()]

def additions(cur, body):
    """Word-level: bill (brackets removed) minus current statute. No length floor."""
    b = re.sub(r'\[[^\[\]]*\]', ' ', body)
    a_w, b_w = cur.split(), re.sub(r'\s+', ' ', b).split()
    sm = difflib.SequenceMatcher(None, a_w, b_w, autojunk=False)
    adds = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag in ('insert', 'replace'):
            frag = ' '.join(b_w[j1:j2])
            if re.search(r'[A-Za-z]{3,}|\d', frag):
                adds.append(frag)
    return adds, sm.ratio()

def audit(bill, date, enacted=None):
    secs = bill_sections(bill, date)
    b = BILLS[norm(bill)]
    enacted = (b['status'] == 4) if enacted is None else enacted
    print(f"{'='*72}\n{norm(bill)}  status={b['status']}  {'ENACTED — ADDED is empty by construction; DELETED still valid' if enacted else 'not enacted — full test'}")
    for sec, body in secs.items():
        dels = deletions(body)
        cur = statute(sec)
        if len(cur) < 80:
            print(f"  {sec}: NEW SECTION (no current statute) — {len(body.split())} words, all new")
            continue
        adds, ratio = additions(cur, body)
        print(f"  {sec}: {len(body.split())} words | similarity to current {ratio:.3f} | DELETED {len(dels)} | ADDED {len(adds)}")
        for d in dels[:6]:
            print(f"      - [{d[:240]}]")
        for a in adds[:8]:
            print(f"      + {a[:240]}")

if __name__ == '__main__':
    audit(sys.argv[1], sys.argv[2])
