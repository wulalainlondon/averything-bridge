import importlib.util,unittest,tempfile,json,hashlib,os,subprocess
from pathlib import Path
spec=importlib.util.spec_from_file_location('guard',Path(__file__).with_name('check_release_services.py'));g=importlib.util.module_from_spec(spec);spec.loader.exec_module(g)
class ServiceGuard(unittest.TestCase):
 def fixture(self):
  d=tempfile.TemporaryDirectory();self.addCleanup(d.cleanup);root=Path(d.name);binary=root/'candidate';binary.write_bytes(b'fixture');cfg=root/'line-service.json';cfg.write_text(json.dumps({'version':1,'enabled':True}));cfg.chmod(0o600);return root,binary,hashlib.sha256(binary.read_bytes()).hexdigest()
 def test_known_new_candidate_matches_enabled_config(self):
  root,binary,sha=self.fixture();seen=[]
  def run(args,**kw):seen.append(args);return subprocess.CompletedProcess(args,0,json.dumps({'ok':True,'line_enabled':True,'bundled_resources_verified':True,'account_network_effect':False}))
  self.assertTrue(g.check(binary,root,sha,run)['line_required']);self.assertIn('--check-line-config',seen[0])
 def test_old_binary_refusal_is_failure_and_no_positional_start(self):
  root,binary,sha=self.fixture()
  def run(args,**kw):self.assertEqual(args[1],'--check-line-config');return subprocess.CompletedProcess(args,2,'')
  with self.assertRaises(ValueError):g.check(binary,root,sha,run)
 def test_existing_store_cannot_hide_missing_config(self):
  root,binary,sha=self.fixture();(root/'line-service.json').unlink();(root/'line').mkdir();(root/'line/line.sqlite').write_bytes(b'existing')
  with self.assertRaises(ValueError):g.check(binary,root,sha,lambda *a,**k:self.fail('must not run candidate'))
 def test_ignored_config_or_changed_binary_rejected(self):
  root,binary,sha=self.fixture()
  with self.assertRaises(ValueError):g.check(binary,root,sha,lambda *a,**k:subprocess.CompletedProcess(a,0,json.dumps({'ok':True,'line_enabled':False})))
  binary.write_bytes(b'changed')
  with self.assertRaises(ValueError):g.check(binary,root,sha,lambda *a,**k:self.fail('hash mismatch must not execute'))
if __name__=='__main__':unittest.main()
