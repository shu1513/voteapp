import json,collections,os,re
S=os.path.dirname(os.path.abspath(__file__))
pool=json.load(open(S+'/pool.json'))
# one roll per measure per chamber: keep the LATEST version-passing roll in each chamber
best={}
for m,rows in pool.items():
    per={}
    for r in rows:
        c=r['chamber']
        if c not in per or r['date']>per[c]['date']: per[c]=r
    best[m]=per
# value = Assembly roll counts ~6x (80/80 seats up vs 20/40)
def value(per): return sum(6 if c=='A' else 1 for c in per)
BUDGET=re.compile(r'\bBudget Act\b|Budget Acts of',re.I)
rank=[]
for m,per in best.items():
    title=list(per.values())[0]['title']
    rank.append(dict(measure=m,value=value(per),chambers=''.join(sorted(per)),
        status=list(per.values())[0]['status'],
        rolls={c:{'roll':r['roll'],'date':r['date'],'yea':r['yea'],'nay':r['nay']} for c,r in per.items()},
        budget=bool(BUDGET.search(title)),title=title[:150]))
rank.sort(key=lambda x:(-x['value'],x['measure']))
live=[r for r in rank if not r['budget']]
print('ranked',len(rank),'| budget-package (drop on sight)',len(rank)-len(live))
c=collections.Counter(r['chambers'] for r in live); print('by chambers',c.most_common())
print('both-chamber A+S available:',sum(1 for r in live if r['chambers']=='AS'))
json.dump(rank,open(S+'/worklist.json','w'),indent=1)
for r in live[:12]: print(r['value'],r['chambers'],r['measure'],r['status'],'|',r['title'][:70])
