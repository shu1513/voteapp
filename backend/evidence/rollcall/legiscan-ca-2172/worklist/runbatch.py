"""Batch driver for the CA roll-call campaign.
Usage:
  python3 runbatch.py judge  <batchdir>     # lint + judge dry-run + judge
  python3 runbatch.py import <batchdir>     # import dry-run + import + rerun + reconcile
Keeps every check the method requires and fails closed.
"""
import json,subprocess,sys,os,re
B='/Users/shu/voteApp/.claude/worktrees/hungry-engelbart-5b1814/backend'
C=B+'/evidence/rollcall/legiscan-ca-2172'
DB='postgresql://localhost:5432/voteapp'
S=os.path.dirname(os.path.abspath(__file__))
def sh(c):
    r=subprocess.run(c,shell=True,cwd=B,capture_output=True,text=True); return r.stdout+r.stderr
def js(s):
    i=s.find('{')
    while i!=-1:
        try: return json.loads(s[i:s.rindex('}')+1])
        except Exception: i=s.find('{',i+1)
    return None
def live():
    return int(sh(f"psql {DB} -tAc \"select count(*) from candidate_records where origin_run_id like 'rollcall:CA:%' and retired_at is null\"").strip())
def imp(dry):
    return sh(f"npm run --silent rollcall:legiscan:import -- --state CA --evidence-dir /Users/shu/legiscan-data/ca-2172-evidence "
              f"--crosswalk-file {C}/crosswalk.json --people-file {C}/legiscan-people-ca-2172.json --scope-from 2026-11-01"+(" --dry-run" if dry else ""))
cmd,bd=sys.argv[1],sys.argv[2]
jf=os.path.join(bd,'judgments.json')
if cmd=='judge':
    out=sh(f"npx tsx {S}/lint.mjs {jf}")
    print(out.strip())
    if 'WARN' in out or 'SPELL' in out: sys.exit('LINT FAILED')
    d=js(sh(f"npm run --silent rollcall:judge -- --judgments-file {jf} --dry-run"))
    if not d: sys.exit('judge dry-run failed')
    print('judge dry-run:',d['counts'])
    d=js(sh(f"npm run --silent rollcall:judge -- --judgments-file {jf}"))
    print('judge:',d['counts'] if d else 'FAILED')
elif cmd=='import':
    before=live(); print('live before',before)
    d=js(imp(True))
    if not d: sys.exit('import dry-run failed')
    ins=d['actions'].get('insert',0); print('dry-run actions',d['actions'])
    d=js(imp(False)); print('import actions',d['actions'],'outcomes',d['outcomes'])
    after=live(); print('live after',after)
    d=js(imp(True)); print('re-run actions',d['actions'])
    ok = (after-before==ins) and not d['actions'].get('insert') and not d['actions'].get('update')
    print('RECONCILE', 'OK' if ok else 'MISMATCH', f'delta={after-before} predicted={ins}')
    if not ok: sys.exit(1)
