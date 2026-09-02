"""Render an Indiana bill PDF to text with ADDED (bold) text wrapped in <<...>>.

Indiana prints amendments in three styles: existing statute text in roman, additions in
bold, deletions in roman with a strikeout rule.  pdftotext flattens all three, which is how
the SB 289 description went wrong.  pdftohtml keeps the bold, so additions become visible
without rendering page images; anything roman that sits next to an addition is either
existing text or a deletion, and only that residue needs a picture.
"""
import subprocess, sys, re, html, collections
def annotate(pdf):
    xml = subprocess.run(["pdftohtml","-xml","-i","-q","-stdout",pdf],
                         capture_output=True, text=True).stdout
    fonts = {m.group(1): ("Bold" in m.group(2)) for m in
             re.finditer(r'<fontspec id="(\d+)"[^>]*family="([^"]*)"', xml)}
    pages = xml.split("<page ")
    out = []
    for pi, page in enumerate(pages[1:], 1):
        items = []
        for m in re.finditer(r'<text top="(\d+)" left="(\d+)"[^>]*font="(\d+)">(.*?)</text>', page, re.S):
            top, left, font, txt = int(m.group(1)), int(m.group(2)), m.group(3), m.group(4)
            txt = html.unescape(re.sub(r"<[^>]+>", "", txt))
            if not txt.strip(): continue
            items.append((top, left, fonts.get(font, False), txt))
        out.append(f"\n--- page {pi} ---")
        # cluster into visual lines: bold and roman runs on one line differ by a pixel or two
        rows, cur = [], []
        for it in sorted(items):
            if cur and it[0] - cur[0][0] > 3: rows.append(cur); cur = []
            cur.append(it)
        if cur: rows.append(cur)
        for row in rows:
            parts, buf, bold = [], "", None
            for _, _, b, t in sorted(row, key=lambda r: r[1]):
                if bold is None or b == bold: buf += t
                else:
                    parts.append((bold, buf)); buf, bold = t, b
                bold = b
            parts.append((bold, buf))
            out.append("".join(f"<<{t}>>" if b else t for b, t in parts))
    return "\n".join(out)
if __name__ == "__main__":
    open(sys.argv[2], "w").write(annotate(sys.argv[1]))
