"""Download every printed version of an Indiana bill straight from the LegiScan dataset.

The dataset's bill JSON carries a dated `state_link` for each version, so there is no need
to guess the `<nn>.<STAGE>` part of an iga.in.gov filename, and the dates make the
roll-to-version mapping exact.  iga.in.gov returns its JavaScript shell (691 bytes) unless
the request carries a browser Accept and Referer.
"""
import json, os, re, subprocess, sys, urllib.request, concurrent.futures
SP=os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0,SP)
from session import BILLDIR, SESSION
UA={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept":"application/pdf,*/*","Referer":"https://iga.in.gov/"}
from annot import annotate
def fetch(url,dest):
    if os.path.exists(dest) and os.path.getsize(dest)>5000: return os.path.getsize(dest)
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=120) as r: b=r.read()
            if len(b)>5000: open(dest,"wb").write(b); return len(b)
        except Exception: pass
    return 0
def run(bills):
    for d in ("pdf","txt","annot"): os.makedirs(f"{SP}/{SESSION}/{d}",exist_ok=True)
    jobs=[]
    for b in bills:
        d=json.load(open(f"{BILLDIR}/{b}.json"))["bill"]
        print(f"\n{b}  {d['title']}")
        for t in d.get("texts",[]):
            name=t["state_link"].rsplit("/",1)[-1]
            print(f"   {t['date']}  {t['type']:12} {name}")
            jobs.append((name,t["state_link"]))
    with concurrent.futures.ThreadPoolExecutor(6) as ex:
        list(ex.map(lambda j: fetch(j[1], f"{SP}/{SESSION}/pdf/{j[0]}"), jobs))
    for name,_ in jobs:
        p=f"{SP}/{SESSION}/pdf/{name}"
        if not os.path.exists(p): print("MISSING",name); continue
        stem=name[:-4]
        if not os.path.exists(f"{SP}/{SESSION}/txt/{stem}.txt"):
            subprocess.run(["pdftotext","-layout",p,f"{SP}/{SESSION}/txt/{stem}.txt"],check=False,stderr=subprocess.DEVNULL)
        if not os.path.exists(f"{SP}/{SESSION}/annot/{stem}.txt"):
            open(f"{SP}/{SESSION}/annot/{stem}.txt","w").write(annotate(p))
if __name__=="__main__": run(sys.argv[1:])
