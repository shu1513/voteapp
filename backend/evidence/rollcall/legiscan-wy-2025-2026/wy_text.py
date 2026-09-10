"""Extract Wyoming enrolled-act text with deletions and insertions marked.

Wyoming prints an amended statute section in full: language being DELETED is
struck through with a horizontal rule, and language being ADDED is underlined.
`pdftotext` keeps both and marks neither, so an extract reads repealed law as
if it were live. This reads the rules out of the PDF and labels every word.
"""
import sys, fitz

def rules(page):
    out = []
    for dr in page.get_drawings():
        for it in dr['items']:
            if it[0] == 'l':
                a, b = it[1], it[2]
                if abs(a.y - b.y) < 1.2 and abs(b.x - a.x) > 3:
                    out.append((min(a.x, b.x), max(a.x, b.x), (a.y + b.y) / 2))
            elif it[0] == 're':
                r = it[1]
                if r.height < 1.6 and r.width > 3:
                    out.append((r.x0, r.x1, (r.y0 + r.y1) / 2))
    return out

HEADER_Y = 130.0  # page furniture: the running head is underlined on every page

def classify_word(w, rl):
    x0, y0, x1, y1 = w[0], w[1], w[2], w[3]
    mid = (y0 + y1) / 2
    if y1 < HEADER_Y:
        return 'same'
    for rx0, rx1, ry in rl:
        if rx1 < x0 + 1 or rx0 > x1 - 1:
            continue
        if abs(ry - mid) <= (y1 - y0) * 0.30:
            return 'del'
        if -2.0 <= ry - y1 <= 3.5:
            return 'new'
    return 'same'

def extract(path):
    doc = fitz.open(path)
    lines = []
    for page in doc:
        rl = rules(page)
        words = page.get_text('words')
        words.sort(key=lambda w: (round(w[3], 1), w[0]))
        cur_y, buf = None, []
        for w in words:
            key = round(w[3], 1)
            if cur_y is not None and abs(key - cur_y) > 1.5:
                lines.append(buf); buf = []
            cur_y = key
            buf.append((w[4], classify_word(w, rl)))
        if buf:
            lines.append(buf)
    doc.close()
    out = []
    for ln in lines:
        parts, run, tag = [], [], None
        for word, t in ln:
            if t != tag:
                if run:
                    parts.append((tag, ' '.join(run)))
                run, tag = [], t
            run.append(word)
        if run:
            parts.append((tag, ' '.join(run)))
        rendered = []
        for t, s in parts:
            if t == 'del':
                rendered.append('[DELETED: ' + s + ']')
            elif t == 'new':
                rendered.append('[NEW: ' + s + ']')
            else:
                rendered.append(s)
        out.append(' '.join(rendered))
    return '\n'.join(out)

if __name__ == '__main__':
    print(extract(sys.argv[1]))
