"""Audit a Missouri description against what the bill ACTUALLY changes.

pdftotext discards bold, and Missouri prints NEW matter in bold. So a plain dump
of an amendment bill shows existing law and the proposal indistinguishably. The
only mechanical test is to diff the bill's version of a section against the
current section on revisor.mo.gov: text present in both is pre-existing law.
"""
import sys, re, os, json, subprocess, difflib, html
sys.path.insert(0, '/Users/shu/legiscan-data/mo-work')
import mo

CACHE = '/Users/shu/legiscan-data/mo-work/rsmo'
os.makedirs(CACHE, exist_ok=True)

def norm(t):
    t = re.sub(r'\s+', ' ', t)
    t = re.sub(r'\b\d{1,3}\b(?= )', ' ', t)          # line numbers
    t = re.sub(r'(HB|SB|HCS|SCS|SS|HJR|SJR)[ \w.]{0,24}?\d+\s*\d*\b', ' ', t)
    return re.sub(r'\s+', ' ', t).strip()

def current(sec):
    p = os.path.join(CACHE, sec + '.txt')
    if os.path.exists(p) and os.path.getsize(p) > 200:
        return open(p, encoding='utf-8').read()
    r = subprocess.run(['curl', '-sSL', f'https://revisor.mo.gov/main/OneSection.aspx?section={sec}'],
                       capture_output=True, text=True)
    t = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', r.stdout, flags=re.S | re.I)
    t = html.unescape(re.sub(r'<[^>]+>', ' ', t))
    i = t.find('Effective')
    body = norm(t)
    open(p, 'w', encoding='utf-8').write(body)
    return body

def amended_sections(bill, date):
    """Sections the bill repeals and re-enacts = amendments to existing law."""
    _, txt = mo.docs(bill, date)
    t = re.sub(r'\s+', ' ', txt)
    m = re.search(r'Section A\.\s*(.{0,900}?)(?:to read as follows|to be known as)', t)
    seg = m.group(1) if m else ''
    secs = re.findall(r'\b(\d+[A-Z]?\.\d+)\b', seg)
    repealed = re.search(r'(.{0,600}?)\s+(?:RSMo,?\s+)?(?:is|are)\s+repealed', seg)
    old = set(re.findall(r'\b(\d+[A-Z]?\.\d+)\b', repealed.group(1))) if repealed else set()
    return t, sorted(old), sorted(set(secs))

def new_matter(bill, date, sec, window=1400):
    """Return the parts of the bill's SEC that are NOT in the current statute."""
    t, _, _ = amended_sections(bill, date)
    m = re.search(re.escape(sec) + r'\.\s+1?\.?(.{0,20000})', t)
    if not m:
        return None
    billsec = norm(m.group(1))[:20000]
    cur = current(sec)
    if len(cur) < 200:
        return ('NO_STATUTE', billsec[:window])
    sm = difflib.SequenceMatcher(None, cur, billsec, autojunk=False)
    adds = [billsec[j1:j2] for tag, i1, i2, j1, j2 in sm.get_opcodes()
            if tag in ('insert', 'replace') and (j2 - j1) > 60]
    return ('OK', adds)

if __name__ == '__main__':
    bill, date = sys.argv[1], sys.argv[2]
    t, old, secs = amended_sections(bill, date)
    print(f'{bill}: repealed/amended sections = {old or "(none — new act)"}')
    for s in old[:int(sys.argv[3]) if len(sys.argv) > 3 else 4]:
        r = new_matter(bill, date, s)
        if not r: print(f'  {s}: (section body not located)'); continue
        kind, adds = r
        if kind == 'NO_STATUTE': print(f'  {s}: NO CURRENT STATUTE (repealed or new); bill text: {adds[:300]}')
        else:
            print(f'  {s}: {len(adds)} new fragment(s)')
            for a in adds[:3]: print(f'      + {a[:400]}')
