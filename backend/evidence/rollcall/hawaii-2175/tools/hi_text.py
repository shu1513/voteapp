"""Flatten a Hawaii bill HTML into text that KEEPS the statutory markup.

Hawaii bills print "Statutory material to be repealed is bracketed and stricken.
New statutory material is underscored." In the HTML, new text sits in <u>...</u>
and struck text in <s>...</s>. A plain text dump loses both, so repealed law
reads as live and additions look like reprinted law. This prints <<new>> and
[[deleted]] instead. Unmarked text is existing law being reprinted.

Usage: hi_text.py <in.html> <out.txt>
"""
import html, re, sys

NEW_OPEN, NEW_CLOSE, DEL_OPEN, DEL_CLOSE = "\x01", "\x02", "\x03", "\x04"

src = open(sys.argv[1], encoding="utf-8", errors="replace").read()
src = re.sub(r"(?is)<(style|script|head)\b.*?</\1>", " ", src)
# Checked on the tags, not on the output markers: the bill text itself prints
# literal [ and ] (bracketed section headings), so marker counts drift.
for tag in ("u", "s"):
    opened = len(re.findall(rf"(?is)<{tag}\b[^>]*>", src))
    closed = len(re.findall(rf"(?is)</{tag}\s*>", src))
    if opened != closed:
        sys.exit(f"{sys.argv[1]}: {opened} <{tag}> but {closed} </{tag}>; markup cannot be trusted, nothing written")
# Mark the two semantic tags with control characters so the tag stripper
# below cannot eat them (angle-bracket markers were stripped as tags).
src = re.sub(r"(?is)<u\b[^>]*>", NEW_OPEN, src)
src = re.sub(r"(?is)</u\s*>", NEW_CLOSE, src)
src = re.sub(r"(?is)<s\b[^>]*>", DEL_OPEN, src)
src = re.sub(r"(?is)</s\s*>", DEL_CLOSE, src)
src = re.sub(r"(?is)<br\s*/?>", "\n", src)
src = re.sub(r"(?is)</(p|div|tr|li|h\d)\s*>", "\n", src)
src = re.sub(r"(?is)<[^>]+>", "", src)
text = html.unescape(src).replace("\xa0", " ")
text = text.replace(NEW_OPEN, "<<").replace(NEW_CLOSE, ">>").replace(DEL_OPEN, "[[").replace(DEL_CLOSE, "]]")
text = re.sub(r"[ \t\r\f\v]+", " ", text)
text = re.sub(r"\n\s*\n+", "\n\n", text)
text = re.sub(r"<<\s*>>", "", text)
text = re.sub(r"\[\[\s*\]\]", "", text)
open(sys.argv[2], "w", encoding="utf-8").write(text.strip() + "\n")
print(sys.argv[2], "chars", len(text), "new-runs", len(re.findall(r"<<", text)), "deleted-runs", len(re.findall(r"\[\[", text)))
