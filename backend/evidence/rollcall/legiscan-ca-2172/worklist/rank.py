import json,collections,os,re
S=os.path.dirname(os.path.abspath(__file__))
pool=json.load(open(S+'/pool.json'))
# A procedural roll (lay an amendment on the table, appeal the chair,
# reconsider) is a vote on a motion, not on the bill's text, so it is not a
# stance on the measure and leaves the pool. SB 106 and AB 100 have only a
# divided procedural roll and drop out here entirely.
# Among what remains, same-day rolls are common: take the latest date, then
# the highest roll id, and keep every same-day alternative on the entry so
# the batch worker has to look at it.
PROCEDURAL=re.compile(r'Motion to Lay on the Table|Appeal the Decision|Amend By|Motion to Reconsider|Reconsideration',re.I)
def key(r): return (r['date'], r['roll'])
best={}
for m,rows in pool.items():
    per=collections.defaultdict(list)
    for r in rows:
        if not PROCEDURAL.search(r['desc']): per[r['chamber']].append(r)
    if per: best[m]={c:sorted(rs,key=key,reverse=True) for c,rs in per.items()}
print('measures with only procedural divided rolls, dropped:',len(pool)-len(best))
def value(per): return sum(6 if c=='A' else 1 for c in per)
# Budget vehicles have no single nameable subject; batch-07 dropped 12 of them.
PKG=re.compile(r'\bBudget Act\b|Budget Acts of|trailer bill|omnibus|\bappropriation[s]?\b.*\bbudget\b',re.I)
VAGUE=re.compile(r'^(State government|Public safety|Human services|General government|Education finance|Health|Taxation)\.?$',re.I)
rank=[]
for m,per in best.items():
    title=next(iter(per.values()))[0]['title']
    rolls={}; ties={}
    for c,rs in per.items():
        top=rs[0]
        rolls[c]={'roll':top['roll'],'date':top['date'],'yea':top['yea'],'nay':top['nay'],'desc':top['desc'][:120]}
        same=[r for r in rs[1:] if r['date']==top['date']]
        if same: ties[c]=[{'roll':r['roll'],'yea':r['yea'],'nay':r['nay'],'desc':r['desc'][:120]} for r in same]
    rank.append(dict(measure=m,value=value(per),chambers=''.join(sorted(per)),
        status=next(iter(per.values()))[0]['status'],rolls=rolls,
        same_day_alternatives=ties or None,
        pkg=bool(PKG.search(title)),vague=bool(VAGUE.search(title.strip())),title=title[:150]))
rank.sort(key=lambda x:(-x['value'],x['measure']))
json.dump(rank,open(S+'/worklist.json','w'),indent=1)
live=[r for r in rank if not r['pkg']]
print('ranked',len(rank),'| pkg dropped',len(rank)-len(live),'| live',len(live))
print('live by chambers',collections.Counter(r['chambers'] for r in live).most_common())
print('live with an Assembly roll',sum(1 for r in live if 'A' in r['chambers']))
print('live vague-title',sum(1 for r in live if r['vague']))
print('entries with same-day alternatives',sum(1 for r in rank if r['same_day_alternatives']))
for r in rank:
    if r['same_day_alternatives']: print(' ',r['measure'],{c:(v['roll'],v['desc'][:50]) for c,v in r['rolls'].items() if c in r['same_day_alternatives']})
