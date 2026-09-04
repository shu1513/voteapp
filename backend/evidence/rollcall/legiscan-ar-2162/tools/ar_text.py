"""Arkansas act reader.

Arkansas prints statutory amendments in place: deleted words are struck
through and new words are underlined, and both come out of a plain text
extraction looking identical. Reading the extract alone can invert an act's
meaning. This renders each line with deletions in [[...]] and additions in
<<...>>, by asking the PDF where its drawn lines sit relative to each
character: a line crossing a character's middle is a strikethrough, a line
just below its baseline is an underline.

Usage:  python3 ar_text.py <doc_id> [first_page] [last_page]
        python3 ar_text.py --raw <doc_id>     (no markup, faster)
Docs are cached under /Users/shu/legiscan-data/ar-docs/.
"""
import os, sys, json, base64, urllib.request, subprocess

CACHE = '/Users/shu/legiscan-data/ar-docs'
os.makedirs(CACHE, exist_ok=True)


def fetch(doc_id):
    pdf = os.path.join(CACHE, f'{doc_id}.pdf')
    if not os.path.exists(pdf):
        env = {}
        for line in open('/Users/shu/voteApp/backend/.env'):
            if line.startswith('LEGISCAN_API_KEY='):
                env['k'] = line.split('=', 1)[1].strip().strip('"')
        url = f"https://api.legiscan.com/?key={env['k']}&op=getBillText&id={doc_id}"
        d = json.load(urllib.request.urlopen(url, timeout=120))
        if d.get('status') != 'OK':
            raise SystemExit(f'legiscan getBillText failed for {doc_id}: {d}')
        t = d['text']
        raw = base64.b64decode(t['doc'])
        if len(raw) != t['text_size']:
            raise SystemExit(f'size mismatch: got {len(raw)}, header says {t["text_size"]}')
        open(pdf, 'wb').write(raw)
        json.dump({x: t[x] for x in t if x != 'doc'}, open(pdf + '.meta.json', 'w'), indent=1)
    return pdf


def marked(pdf, first=None, last=None):
    from pdfminer.high_level import extract_pages
    from pdfminer.layout import LTChar, LTTextContainer, LTRect, LTLine, LTTextLine, LAParams
    pages = range(first - 1, last) if first else None
    out = []
    for page in extract_pages(pdf, page_numbers=pages, laparams=LAParams(detect_vertical=False)):
        rules = []          # (x0, x1, y) of every thin horizontal drawn line
        lines = []
        def walk(obj, sink):
            for el in obj:
                if isinstance(el, (LTRect, LTLine)):
                    h = el.y1 - el.y0
                    if h <= 2.5 and (el.x1 - el.x0) > 1:
                        rules.append((el.x0, el.x1, (el.y0 + el.y1) / 2))
                elif isinstance(el, LTTextLine):
                    lines.append(el)
                elif isinstance(el, LTTextContainer):
                    walk(el, sink)
        walk(page, None)
        for ln in sorted(lines, key=lambda l: -l.y0):
            buf, mode = [], None
            for ch in ln:
                if not isinstance(ch, LTChar):
                    if buf and buf[-1] != ' ':
                        buf.append(' ')
                    continue
                base, h = ch.y0, max(ch.y1 - ch.y0, 0.01)
                # Measured on Act 116: a strikethrough rule sits about 0.54
                # of the glyph height above the baseline, an underline about
                # 0.13 above it. The bands below are wide enough for both and
                # do not overlap.
                m = None
                for x0, x1, y in rules:
                    if x1 < ch.x0 - 0.5 or x0 > ch.x1 + 0.5:
                        continue
                    rel = (y - base) / h
                    if 0.32 <= rel <= 0.80:
                        m = 'del'; break
                    if -0.30 <= rel <= 0.28:
                        m = 'add'
                if m != mode:
                    if mode == 'del': buf.append(']]')
                    if mode == 'add': buf.append('>>')
                    if m == 'del': buf.append('[[')
                    if m == 'add': buf.append('<<')
                    mode = m
                buf.append(ch.get_text())
            if mode == 'del': buf.append(']]')
            if mode == 'add': buf.append('>>')
            out.append(''.join(buf).rstrip())
    return '\n'.join(out)


if __name__ == '__main__':
    a = sys.argv[1:]
    raw = a and a[0] == '--raw'
    if raw: a = a[1:]
    # A local path may be given instead of a LegiScan doc id, for a document
    # LegiScan does not carry (Arkansas joint resolutions have no chaptered
    # text, so their adopted print comes straight from arkleg).
    pdf = a[0] if a[0].endswith('.pdf') else fetch(int(a[0]))
    if raw:
        subprocess.run(['pdftotext', '-layout', pdf, '-'])
    else:
        first = int(a[1]) if len(a) > 1 else None
        last = int(a[2]) if len(a) > 2 else first
        print(marked(pdf, first, last))
