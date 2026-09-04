"""Batch driver for the CA roll-call campaign.
Usage:
  python3 runbatch.py judge  <batchdir>     # lint + judge dry-run + judge
  python3 runbatch.py import <batchdir>     # import dry-run + import + rerun + reconcile
Keeps every check the method requires and fails closed: a non-zero exit from any
command, a lint warning, a failure outcome in the import report, or an
ambiguous plan stops the run.
"""
import json,subprocess,sys,os
S=os.path.dirname(os.path.abspath(__file__))
B=os.path.abspath(os.path.join(S,'..','..','..','..'))          # backend/
C=B+'/evidence/rollcall/legiscan-ca-2172'
DB='postgresql://localhost:5432/voteapp'
EVIDENCE=os.path.expanduser('~/legiscan-data/ca-2172-evidence')
FAIL_OUTCOMES=('error','source_mismatch')
BAD_ACTIONS=('ambiguous',)
def sh(c):
    r=subprocess.run(c,shell=True,cwd=B,capture_output=True,text=True)
    out=r.stdout+r.stderr
    if r.returncode!=0:
        sys.exit(f"COMMAND FAILED ({r.returncode}): {c}\n{out[-2000:]}")
    return out
def js(s):
    i=s.find('{')
    while i!=-1:
        try: return json.loads(s[i:s.rindex('}')+1])
        except Exception: i=s.find('{',i+1)
    return None
def live():
    return int(sh(f"psql {DB} -tAc \"select count(*) from candidate_records where origin_run_id like 'rollcall:CA:%' and retired_at is null\"").strip())
def imp(dry):
    d=js(sh(f"npm run --silent rollcall:legiscan:import -- --state CA --evidence-dir {EVIDENCE} "
            f"--crosswalk-file {C}/crosswalk.json --people-file {C}/legiscan-people-ca-2172.json --scope-from 2026-11-01"+(" --dry-run" if dry else "")))
    if not d: sys.exit('import produced no report')
    bad={k:v for k,v in d['outcomes'].items() if k in FAIL_OUTCOMES and v}
    bad.update({k:v for k,v in d['actions'].items() if k in BAD_ACTIONS and v})
    if bad: sys.exit(f"IMPORT FAILURES {bad}")
    return d
cmd,bd=sys.argv[1],os.path.abspath(sys.argv[2])
jf=os.path.join(bd,'judgments.json')
if cmd=='judge':
    out=sh(f"npx tsx {S}/lint.mjs {jf}")
    print(out.strip())
    if 'WARN' in out or 'SPELL' in out: sys.exit('LINT FAILED')
    d=js(sh(f"npm run --silent rollcall:judge -- --judgments-file {jf} --dry-run"))
    if not d: sys.exit('judge dry-run failed')
    print('judge dry-run:',d['counts'])
    d=js(sh(f"npm run --silent rollcall:judge -- --judgments-file {jf}"))
    if not d: sys.exit('judge failed')
    print('judge:',d['counts'])
elif cmd=='import':
    before=live(); print('live before',before)
    d=imp(True)
    ins=d['actions'].get('insert',0); print('dry-run actions',d['actions'])
    d=imp(False); print('import actions',d['actions'],'outcomes',d['outcomes'])
    after=live(); print('live after',after)
    d=imp(True); print('re-run actions',d['actions'])
    pending={k:v for k,v in d['actions'].items() if k in ('insert','rewrite') and v}
    ok = (after-before==ins) and not pending
    print('RECONCILE', 'OK' if ok else 'MISMATCH', f'delta={after-before} predicted={ins} pending={pending}')
    if not ok: sys.exit(1)
else:
    sys.exit(f'unknown command {cmd}')
