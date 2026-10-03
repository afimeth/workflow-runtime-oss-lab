"""Execute checks and emit an exact-commit measurement. No hard-coded test totals."""
import datetime,hashlib,json,os,platform,re,subprocess,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];os.chdir(ROOT);sys.path.insert(0,str(ROOT))
claims=json.loads(Path('evidence/claims.json').read_text())
checks=[]
def run(cmd):
 p=subprocess.run(cmd,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=180)
 checks.append({'command':[Path(cmd[0]).name,*cmd[1:]],'exit_code':p.returncode,'stdout_sha256':hashlib.sha256(p.stdout.encode()).hexdigest(),'stderr_sha256':hashlib.sha256(p.stderr.encode()).hexdigest()})
 if p.returncode:
  print(p.stdout[-12000:]);print(p.stderr[-6000:]);raise RuntimeError('CHECK_FAILED')
 return p.stdout
def npm(*args):return run(['npm.cmd' if os.name=='nt' else 'npm',*args])
kind=claims['kind'];summary={};tools={};ok=False
try:
 if kind=='python':
  suite=unittest.defaultTestLoader.discover('tests');result=unittest.TextTestRunner(verbosity=2).run(suite)
  summary={'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped)}
  tools={'python':platform.python_version(),'sqlite':__import__('sqlite3').sqlite_version}
  if not result.wasSuccessful():raise RuntimeError('TESTS_FAILED')
  run([sys.executable,'app.py','demo'])
  if 'durable' in ROOT.name:
   out=run([sys.executable,'app.py','benchmark']);Path('evidence/benchmark.json').write_text(out,encoding='utf-8')
 elif kind=='go':
  tools={'go':run(['go','version']).strip()};events=[json.loads(x) for x in run(['go','test','-json','-count=1','./...']).splitlines() if x.startswith('{')]
  summary={'tests':sum(e.get('Action')=='pass' and 'Test' in e for e in events),'failed':sum(e.get('Action')=='fail' and 'Test' in e for e in events)}
  run(['go','vet','./...'])
  if os.getenv('CI') and platform.system()=='Linux':run(['go','test','-race','-count=1','./...'])
  bench=run(['go','test','-bench','BenchmarkDAG','-benchtime=5x','-run','^$','./...']);Path('evidence/benchmark.txt').write_text(bench,encoding='utf-8')
 elif kind=='forge':
  tools={'forge':run(['forge','--version']).strip(),'solc':'0.8.28','profile':os.getenv('FOUNDRY_PROFILE','default')}
  data=json.loads(run(['forge','test','--json']));results=[(name,r) for s in data.values() for name,r in s['test_results'].items()]
  summary={'reported_tests':len(results),'passed':sum(r['status']=='Success' for _,r in results),'tests':[{ 'name':n,'status':r['status'],'kind':r.get('kind'),'invariant_predicates':r.get('invariant_predicate_results')} for n,r in results]}
  if summary['passed']!=len(results):raise RuntimeError('TESTS_FAILED')
  run(['forge','snapshot','--snap','evidence/gas-snapshot.txt']);Path('evidence/gas-report.txt').write_text(run(['forge','test','--gas-report']),encoding='utf-8')
  run(['forge','script','script/Deploy.s.sol:Deploy'])
 elif kind=='node':
  tools={'node':run(['node','--version']).strip(),'npm':npm('--version').strip()}
  npm('test','--','--reporter=json','--outputFile=evidence/unit-tests.json');unit=json.loads(Path('evidence/unit-tests.json').read_text())
  npm('run','build');npm('run','budget');npm('run','test:e2e');browser=json.loads(Path('evidence/browser-tests.json').read_text())
  summary={'unit_tests':unit['numTotalTests'],'unit_passed':unit['numPassedTests'],'browser_expected':browser['stats']['expected'],'browser_unexpected':browser['stats']['unexpected'],'browser_flaky':browser['stats']['flaky'],'performance':json.loads(Path('evidence/performance-budget.json').read_text())}
  if not unit['success'] or browser['stats']['unexpected'] or browser['stats']['flaky']:raise RuntimeError('TESTS_FAILED')
  npm('audit','--audit-level=moderate')
 ok=True
finally:
 sha=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip();dirty=bool(subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],text=True).strip())
 receipt={'schema':'interview-lab-evidence/v1','measured_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'commit_sha':sha,'tracked_worktree_dirty':dirty,'platform':platform.system(),'architecture':platform.machine(),'toolchain':tools,'summary':summary,'checks':checks,'passed':ok,'safe_cv_claim':claims['safe_cv_claim'],'unsupported_claims':claims['NOT_CLAIMED'],'ci_run_url':f"https://github.com/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{os.getenv('GITHUB_RUN_ID')}" if os.getenv('GITHUB_RUN_ID') else None}
 Path('evidence/receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8');print(json.dumps({'passed':ok,'commit_sha':sha,'summary':summary},indent=2))
if not ok:sys.exit(1)
