"""Wisconsin act reader: shows what the act DELETED and what it ADDED.

Wisconsin amends a statute in place. Deleted words are struck through and new
words are underlined, and a plain text extraction renders both as ordinary text,
run together. Reading that extract can invert an act: on 2025 Act 42 it reads
"a copy of the rules policy under par. (a)", where "rules" is being deleted and
"policy" added.

The marks are thin FILLED RECTANGLES, not line objects, so pdfminer's rectangle
walk does not see them. PyMuPDF's get_drawings does.

Measured on 2025 Act 42, where the answer was known independently: relative to a
character's box, measured from its top and divided by its height,
  a strikethrough sits at about 0.50   -> the word is being DELETED
  an underline    sits at about 0.85   -> the word is being ADDED
Everything unmarked is existing law being reprinted, not a change.

Output marks deletions [[like this]] and additions <<like this>>.

Usage:
  wi_text.py <bill> [Enrolled|Introduced|Chaptered] [first_page] [last_page]
  wi_text.py --check <bill>     report how much of the act is marked
"""
import subprocess
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import wi_docs  # noqa: E402

STRIKE = (0.30, 0.68)
UNDER = (0.68, 1.10)


def read(path, first=None, last=None):
    import fitz
    doc = fitz.open(path)
    pages = range(len(doc)) if first is None else range(first - 1, min(last or first, len(doc)))
    out = []
    for pno in pages:
        page = doc[pno]
        marks = [d['rect'] for d in page.get_drawings()
                 if 2 < d['rect'].width < 260 and d['rect'].height < 3]
        blocks = []
        for blk in page.get_text('rawdict')['blocks']:
            for ln in blk.get('lines', []):
                chars = [c for sp in ln['spans'] for c in sp['chars']]
                if chars:
                    blocks.append((chars[0]['bbox'][1], chars[0]['bbox'][0], chars))
        # Two-column layout: read the left column of the page, then the right.
        width = page.rect.width
        left = sorted([b for b in blocks if b[1] < width / 2], key=lambda b: b[0])
        right = sorted([b for b in blocks if b[1] >= width / 2], key=lambda b: b[0])
        for column in (left, right):
            for _y, _x, chars in column:
                buf, mode = [], None
                for c in chars:
                    x0, y0, x1, y1 = c['bbox']
                    h = max(y1 - y0, 0.01)
                    found = None
                    for r in marks:
                        if r.x1 < x0 + 0.3 or r.x0 > x1 - 0.3:
                            continue
                        rel = (r.y0 - y0) / h
                        if STRIKE[0] <= rel < STRIKE[1]:
                            found = 'del'
                            break
                        if UNDER[0] <= rel <= UNDER[1]:
                            found = 'add'
                            break
                    if found != mode:
                        if mode == 'del':
                            buf.append(']]')
                        elif mode == 'add':
                            buf.append('>>')
                        if found == 'del':
                            buf.append('[[')
                        elif found == 'add':
                            buf.append('<<')
                        mode = found
                    buf.append(c['c'])
                if mode == 'del':
                    buf.append(']]')
                elif mode == 'add':
                    buf.append('>>')
                out.append(''.join(buf).rstrip())
        out.append(f'--- end of page {pno + 1} ---')
    return out


def main():
    if sys.argv[1] == '--check':
        b = wi_docs.bill(sys.argv[2])
        t = [x for x in b['texts'] if x['type'] == 'Enrolled'] or \
            [x for x in b['texts'] if x['type'] == 'Chaptered']
        path = wi_docs.fetch(t[-1]['doc_id'], t[-1].get('text_size'), t[-1].get('text_hash'))
        lines = read(path)
        body = '\n'.join(lines)
        print(f"{sys.argv[2]}: {body.count('[[')} deleted runs, {body.count('<<')} added runs, "
              f"{len(lines)} lines")
        return
    number = sys.argv[2] if sys.argv[1] == '--' else sys.argv[1]
    want = sys.argv[2] if len(sys.argv) > 2 else 'Enrolled'
    first = int(sys.argv[3]) if len(sys.argv) > 3 else None
    last = int(sys.argv[4]) if len(sys.argv) > 4 else None
    b = wi_docs.bill(number)
    hit = [x for x in b['texts'] if x['type'] == want]
    if not hit:
        raise SystemExit(f'{number} has no {want}; it has {[x["type"] for x in b["texts"]]}')
    t = hit[-1]
    path = wi_docs.fetch(t['doc_id'], t.get('text_size'), t.get('text_hash'))
    print('\n'.join(read(path, first, last)))


if __name__ == '__main__':
    main()
