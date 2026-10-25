#!/usr/bin/env python3
"""Check operator-enabled services before replacing the current Go release.
No account credentials, network actions, messages, schedules or LINE calls.
"""
from pathlib import Path
import argparse, hashlib, json, os, stat, subprocess, sys

def check(candidate, data_dir, expected_sha, runner=subprocess.run):
    candidate=Path(candidate);root=Path(data_dir);config=root/'line-service.json'
    if hashlib.sha256(candidate.read_bytes()).hexdigest()!=expected_sha:
        raise ValueError('candidate_binary_hash_changed')
    if not config.exists():
        if (root/'line/line.sqlite').exists():raise ValueError('line_configuration_missing_existing_store')
        return {'ok':True,'line_required':False,'reason':'no_config_or_existing_LINE_store'}
    info=config.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode)!=0o600 or info.st_uid!=os.getuid():raise ValueError('line_private_configuration_required')
    before=config.read_bytes();spec=json.loads(before)
    if spec.get('version')!=1 or type(spec.get('enabled'))is not bool:raise ValueError('line_configuration_invalid')
    # Unknown flag fails before startup on an old binary. Never run an unknown
    # positional subcommand that an older Go could treat as normal server start.
    result=runner([str(candidate),'--check-line-config','--data-dir',str(root)],capture_output=True,text=True,timeout=30)
    if result.returncode!=0:raise ValueError('candidate_LINE_config_or_runtime_preflight_failed')
    reply=json.loads(result.stdout)
    if reply.get('ok')is not True or reply.get('line_enabled')is not spec['enabled'] or reply.get('bundled_resources_verified')is not spec['enabled'] or reply.get('account_network_effect')is not False:raise ValueError('candidate_did_not_preserve_LINE_configuration')
    if config.read_bytes()!=before:raise ValueError('LINE_config_changed_during_preflight')
    return {'ok':True,'line_required':spec['enabled'],'config_sha256':hashlib.sha256(before).hexdigest(),'candidate_binary_sha256':expected_sha,'account_network_effect':False}

def main():
    p=argparse.ArgumentParser();p.add_argument('--candidate',required=True);p.add_argument('--data-dir',required=True);p.add_argument('--expected-binary-sha256',required=True);p.add_argument('--exec',dest='exec_args',nargs=argparse.REMAINDER);a=p.parse_args()
    try:result=check(a.candidate,a.data_dir,a.expected_binary_sha256)
    except Exception as e:print(json.dumps({'ok':False,'error':str(e)[:160]}));return 1
    if a.exec_args:
        if not a.exec_args[0].startswith('/') or not all(isinstance(x,str) for x in a.exec_args):raise ValueError('absolute_launch_program_required')
        os.execv(a.exec_args[0],a.exec_args)
    print(json.dumps(result));return 0
if __name__=='__main__':sys.exit(main())
