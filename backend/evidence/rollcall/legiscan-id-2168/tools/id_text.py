"""Idaho act reader that keeps the strike/underline distinction.

Idaho prints new language UNDERLINED and deleted language STRUCK THROUGH, and
pdftotext renders both as ordinary text, so a plain extract shows repealed law
as live and can invert an act. Same family as the Arkansas technique: resolve
the marks from the PDF's own drawn lines by their height above the baseline.

  <<added>>    underlined  (rule sits just BELOW the baseline)
  [[deleted]]  struck      (rule crosses the glyph box, near mid-height)
"""
import sys, os, subprocess, urllib.request, re
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer, LTChar, LTLine, LTRect

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124 Safari/537.36")
CACHE = os.environ.get("ID_DOC_CACHE", os.path.join(os.path.expanduser("~"), "legiscan-data", "id-docs"))

def fetch(year, name):
    os.makedirs(CACHE, exist_ok=True)
    pdf = f"{CACHE}/{year}-{name}.pdf"
    if not os.path.exists(pdf):
        url = f"https://legislature.idaho.gov/wp-content/uploads/sessioninfo/{year}/legislation/{name}.pdf"
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        open(pdf, "wb").write(urllib.request.urlopen(req, timeout=60).read())
    return pdf

def rules_on_page(page):
    out = []
    for el in page:
        if isinstance(el, LTLine):
            if abs(el.y1 - el.y0) < 1.2 and (el.x1 - el.x0) > 1.0:
                out.append((el.x0, el.x1, (el.y0 + el.y1) / 2.0))
        elif isinstance(el, LTRect):
            h = el.y1 - el.y0
            if h < 1.6 and (el.x1 - el.x0) > 1.0:
                out.append((el.x0, el.x1, (el.y0 + el.y1) / 2.0))
    return out

def mark_for(ch, rules):
    """Return 'add', 'del' or None for one character."""
    h = ch.height or 10.0
    base = ch.y0
    cx0, cx1 = ch.x0, ch.x1
    for x0, x1, y in rules:
        if x1 < cx0 - 0.4 or x0 > cx1 + 0.4:
            continue
        rel = (y - base) / h
        if -0.30 <= rel <= 0.16:
            return "add"          # underline sits at or just below the baseline
        if 0.30 <= rel <= 0.72:
            return "del"          # strike crosses the middle of the glyph
    return None

def render(pdf):
    lines_out = []
    for page in extract_pages(pdf):
        rules = rules_on_page(page)
        for el in page:
            if not isinstance(el, LTTextContainer):
                continue
            for line in el:
                if not any(isinstance(c, LTChar) for c in line):
                    continue
                buf, cur, prev = [], None, None
                for c in line:
                    if not isinstance(c, LTChar):
                        buf.append(c.get_text())
                        continue
                    # pdfminer drops inter-word spaces on some Idaho prints;
                    # reinsert one when the horizontal gap is wide enough.
                    if prev is not None and c.x0 - prev.x1 > 0.28 * (c.height or 10.0):
                        buf.append(" ")
                    prev = c
                    m = mark_for(c, rules)
                    if m != cur:
                        if cur == "add": buf.append(">>")
                        elif cur == "del": buf.append("]]")
                        if m == "add": buf.append("<<")
                        elif m == "del": buf.append("[[")
                        cur = m
                    buf.append(c.get_text())
                if cur == "add": buf.append(">>")
                elif cur == "del": buf.append("]]")
                s = "".join(buf).rstrip()
                lines_out.append((round(-line.y0, 1), s))
    text = "\n".join(s for _, s in lines_out)
    text = re.sub(r">><<", "", text)
    text = re.sub(r"\]\]\[\[", "", text)
    return text

if __name__ == "__main__":
    year, bill = sys.argv[1], sys.argv[2]
    suffix = sys.argv[3] if len(sys.argv) > 3 else ""
    out = render(fetch(year, f"{bill}{suffix}"))
    for ln in out.splitlines():
        ln = re.sub(r"^\s*\d{1,2}\s{2,}", "", ln)
        if re.match(r"^\s*(LEGISLATURE OF THE STATE OF IDAHO|Sixty-\w+ Legislature|IN THE (HOUSE|SENATE))", ln):
            continue
        print(ln.rstrip())
