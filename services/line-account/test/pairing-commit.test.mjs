import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {commitStagedLogin} from '../dist/pairing-commit.js';

test('wrong-principal QR preserves the original encrypted login byte for byte',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'line-pair-commit-'));try{
  const stage=join(dir,'stage');await mkdir(stage);const old=Buffer.from('LJS1'+'a'.repeat(64)),next=Buffer.from('LJS1'+'b'.repeat(64));await writeFile(join(dir,'session.enc'),old);await writeFile(join(stage,'session.enc'),next);
  await assert.rejects(commitStagedLogin(dir,stage,'wrong-account','expected-account'),/line_principal_mismatch/);
  assert.deepEqual(await readFile(join(dir,'session.enc')),old);assert.deepEqual(await readFile(join(stage,'session.enc')),next);
 }finally{await rm(dir,{recursive:true,force:true})}
});
test('verified same-principal commit retains one encrypted previous login and atomically replaces the active file',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'line-pair-ok-'));try{
  const stage=join(dir,'stage');await mkdir(stage);const old=Buffer.from('LJS1'+'a'.repeat(64)),next=Buffer.from('LJS1'+'b'.repeat(64));await writeFile(join(dir,'session.enc'),old);await writeFile(join(stage,'session.enc'),next);
  await commitStagedLogin(dir,stage,'same-account','same-account');assert.deepEqual(await readFile(join(dir,'session.enc')),next);assert.deepEqual(await readFile(join(dir,'session.before-reauth.enc')),old);
  await assert.rejects(readFile(join(stage,'session.enc')),e=>e.code==='ENOENT');
 }finally{await rm(dir,{recursive:true,force:true})}
});
