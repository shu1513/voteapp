"""Verify each selected roll's member list against Indiana's official roll-call PDF.

LegiScan's Indiana member lists disagree with the journal on 30 of the session's 1,010
rolls, and the disagreement is a member recorded on the wrong side, not merely a wrong
count.  Every roll used in a batch is checked name by name before it is judged.
"""
import json, re, subprocess, os, sys, urllib.request, collections
SP=os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0,SP)
from session import VOTEDIR, PEOPLEDIR, YEAR, SESSION
PEOPLE={p["people_id"]:p for p in
        [json.loads(open(f"{PEOPLEDIR}/{f}").read())["person"] for f in os.listdir(PEOPLEDIR)]}
UA={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept":"application/pdf,*/*","Referer":"https://iga.in.gov/"}
def fetch(url,dest):
    if os.path.exists(dest) and os.path.getsize(dest)>5000: return True
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=120) as r: b=r.read()
            if len(b)>5000: open(dest,"wb").write(b); return True
        except Exception: pass
    return False
def sections(txt):
    """Split the roll-call PDF into its YEA / NAY / EXCUSED / NOT VOTING name blocks."""
    txt=re.sub(r'([A-Z]) ([A-Z])', r'\1\2', txt)   # undo the small-caps "Y EA" rendering
    out, cur = {}, None
    for line in txt.splitlines():
        m=re.match(r'\s*(YEA|NAY|EXCUSED|NOTVOTING)\s*-\s*(\d+)', line)
        if m: cur=m.group(1); out[cur]=[]; continue
        if cur is None: continue
        if not line.strip(): continue
        for n in re.split(r'\s{2,}', line.strip()):
            n=n.strip()
            if n and not n.startswith("Presiding"): out[cur].append(n)
    return out
def key(name):
    """Compare on a truncation-tolerant surname plus a first initial when one is printed."""
    # the journal prints the presiding officer as "Mr. Speaker", never by name
    name=name.replace("Mr. Speaker","Huston, T").replace("Mr. President","")
    parts=[p.strip() for p in name.split(",")]
    sur=parts[0].lower().replace("'","").replace(" ","").replace(".","")
    ini=parts[1][0].lower() if len(parts)>1 and parts[1] else ""
    return (sur, ini)
def ls_key(p):
    sur=p["last_name"].lower().replace("'","").replace(" ","").replace(".","")
    return (sur, p["first_name"][0].lower() if p["first_name"] else "")
def prefix_match(a, b):
    """The PDF truncates long surnames in narrow columns, so allow a prefix either way."""
    return (a[0].startswith(b[0]) or b[0].startswith(a[0])) and (not a[1] or not b[1] or a[1]==b[1])
def check(bill, roll_id, journal, origin, resolution=False):
    v=json.load(open(f"{VOTEDIR}/{roll_id}.json"))["roll_call"]
    ch=v["chamber"]
    stem = bill.replace("SJR","SJ") if resolution else bill
    sub = "resolutions" if resolution else "bills"
    name=f"{stem}.{journal}_{ch}.pdf"
    url=f"https://iga.in.gov/pdf-documents/124/{YEAR}/{origin}/{sub}/{stem}/rollcalls/{name}"
    dest=f"{SP}/{SESSION}/rc/{name}"
    os.makedirs(os.path.dirname(dest),exist_ok=True)
    if not fetch(url,dest): return f"{bill} {ch} roll{roll_id}: OFFICIAL PDF NOT FETCHED {url}"
    txt=subprocess.run(["pdftotext","-layout",dest,"-"],capture_output=True,text=True).stdout
    off=sections(txt)
    ls=collections.defaultdict(list)
    for vv in v["votes"]:
        side={"Yea":"YEA","Nay":"NAY","NV":"NOTVOTING","Absent":"EXCUSED"}.get(vv["vote_text"],vv["vote_text"])
        ls[side].append(PEOPLE[vv["people_id"]])
    lines=[f'{bill} {ch} roll{roll_id} journal {journal}: legiscan {v["yea"]}-{v["nay"]}  official {len(off.get("YEA",[]))}-{len(off.get("NAY",[]))}']
    bad=False
    for side in ("YEA","NAY"):
        o=[key(n) for n in off.get(side,[])]
        l=[ls_key(p) for p in ls.get(side,[])]
        used=[False]*len(o); missing=[]
        for lk in l:
            hit=next((i for i,ok in enumerate(o) if not used[i] and prefix_match(lk,ok)), None)
            if hit is None: missing.append(lk)
            else: used[hit]=True
        extra=[o[i] for i in range(len(o)) if not used[i]]
        if missing or extra:
            bad=True
            lines.append(f'   {side}: in LegiScan only {missing} ; in journal only {extra}')
    lines.append("   MATCH" if not bad else "   *** DISAGREEMENT ***")
    return "\n".join(lines)
if __name__=="__main__":
    for row in json.load(open(sys.argv[1])):
        print(check(*row[:4], resolution=row[4] if len(row)>4 else False))
