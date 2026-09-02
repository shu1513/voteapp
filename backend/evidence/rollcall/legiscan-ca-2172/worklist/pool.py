import json,glob,collections
DS='/Users/shu/legiscan-data/ca-2172-0830'; E='/Users/shu/legiscan-data/ca-2172-evidence'
B='/Users/shu/voteApp/.claude/worktrees/hungry-engelbart-5b1814/backend'
worked=set()
for f in glob.glob(B+'/evidence/rollcall/legiscan-ca-2172/batch-*/judgments.json'):
    for j in json.load(open(f))['judgments']: worked.add(j['measure_id'])
bills={}
for f in glob.glob(DS+'/**/bill/*.json',recursive=True):
    d=json.load(open(f)).get('bill')
    if d: bills[d['bill_id']]=d
STATUS={4:'chaptered',3:'enrolled',5:'vetoed'}
def divided(y,n): return n>0 and min(y,n)>=max(y,n)/4.0
rows=[]
for f in glob.glob(E+'/ls-ca-*-roll*.json'):
    d=json.load(open(f)); rc=d['rollCall']; b=bills.get(rc['bill_id'])
    if not b: continue
    st=STATUS.get(b['status'])
    if not st: continue
    if not divided(rc['yea'],rc['nay']): continue
    # final-text/version check: vote on or after the last Amended text
    amend=[t['date'] for t in b.get('texts',[]) if t.get('type')=='Amended']
    ok_version = (not amend) or rc['date']>=max(amend)
    rows.append(dict(measure=d['measureId'],bill_id=rc['bill_id'],status=st,roll=rc['roll_call_id'],
        date=rc['date'],chamber=rc['chamber'],yea=rc['yea'],nay=rc['nay'],desc=rc.get('desc',''),
        version_ok=ok_version,title=b.get('title','')))
bym=collections.defaultdict(list)
for r in rows: bym[r['measure']].append(r)
op={m:v for m,v in bym.items() if m not in worked}
print('worked',len(worked),'| open measures w/ divided final-text floor roll:',len(op))
mix=collections.Counter(tuple(sorted({r['chamber'] for r in v})) for v in op.values())
print('chamber mix',mix.most_common())
# version check applied
opv={m:[r for r in v if r['version_ok']] for m,v in op.items()}
opv={m:v for m,v in opv.items() if v}
print('after version check:',len(opv))
mix2=collections.Counter(tuple(sorted({r['chamber'] for r in v})) for v in opv.values())
print('chamber mix after',mix2.most_common())
st=collections.Counter(v[0]['status'] for v in opv.values()); print('status',st.most_common())
json.dump(opv,open(__import__('os').path.dirname(__file__)+'/pool.json','w'),indent=0)
print('wrote pool.json')
