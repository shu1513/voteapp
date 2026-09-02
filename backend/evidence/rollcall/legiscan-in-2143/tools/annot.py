"""Render an Indiana bill PDF to text with ADDED text in <<...>> and DELETED text in [[...]].

Indiana prints an amendment in three styles and says so on the first page of every act:
existing statute text in roman, additions in bold, and deletions in roman with a rule struck
through them.  `pdftotext` flattens all three into identical plain text, which is how the
batch-01 description of SB 289 came to treat struck words as live law.

Both remaining styles are recoverable.  Additions carry a bold font name.  Deletions are not
a font at all -- the strike is drawn as a thin horizontal curve over the words -- so they are
found by intersecting those curves with the word boxes they cross.

    python3 annot.py bill.pdf out.txt
"""
import sys
import pdfplumber

STRIKE_MAX_HEIGHT = 2.0   # the rule is about 0.7pt tall; anything thicker is a table border
MIN_X_OVERLAP = 0.5       # a word counts as struck when most of it sits under the rule


def _strike_spans(page):
    """Horizontal hairlines, as (top, x0, x1) — these are the strike-through rules."""
    spans = []
    for shape in list(page.curves) + list(page.lines) + list(page.rects):
        if shape["height"] > STRIKE_MAX_HEIGHT and shape["width"] > 1:
            spans.append((shape["top"], shape["x0"], shape["x1"]))
        elif shape["height"] <= STRIKE_MAX_HEIGHT and shape["width"] > 1:
            spans.append((shape["top"], shape["x0"], shape["x1"]))
    return spans


def _is_struck(word, spans):
    for top, x0, x1 in spans:
        # the rule has to run through the word's middle band, not above or below the line
        if not (word["top"] + 1 < top < word["bottom"] - 1):
            continue
        overlap = min(word["x1"], x1) - max(word["x0"], x0)
        if overlap > 0 and overlap / max(word["x1"] - word["x0"], 0.1) >= MIN_X_OVERLAP:
            return True
    return False


def annotate(pdf_path):
    out = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_no, page in enumerate(pdf.pages, 1):
            spans = _strike_spans(page)
            words = page.extract_words(extra_attrs=["fontname"], keep_blank_chars=False)
            out.append(f"\n--- page {page_no} ---")
            line, last_top, state = [], None, None

            def flush():
                if line:
                    out.append(" ".join(line))

            for w in words:
                if last_top is None or abs(w["top"] - last_top) > 3:
                    flush(); line, state = [], None
                    last_top = w["top"]
                if _is_struck(w, spans):
                    kind = "del"
                elif "Bold" in (w.get("fontname") or ""):
                    kind = "add"
                else:
                    kind = None
                if kind != state:
                    if state == "add": line.append(">>")
                    elif state == "del": line.append("]]")
                    if kind == "add": line.append("<<")
                    elif kind == "del": line.append("[[")
                    state = kind
                line.append(w["text"])
            if state == "add": line.append(">>")
            elif state == "del": line.append("]]")
            flush()
    text = "\n".join(out)
    # the markers were appended as separate words; close them up so the text reads normally
    return text.replace("<< ", "<<").replace(" >>", ">>").replace("[[ ", "[[").replace(" ]]", "]]")


if __name__ == "__main__":
    open(sys.argv[2], "w").write(annotate(sys.argv[1]))
