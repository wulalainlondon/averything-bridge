import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,readFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
import {acquireProfileLock} from './profile-lock.mjs';

test('one profile has one owner and release cannot remove a successor',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'line-lock-test-'));try{
  const release=await acquireProfileLock(directory);await assert.rejects(acquireProfileLock(directory),/credential_store_busy/);
  await release();const next=await acquireProfileLock(directory);await release();assert.equal(JSON.parse(await readFile(join(directory,'run.lock'),'utf8')).pid,process.pid);await next();
 }finally{await rm(directory,{recursive:true,force:true})}
});
test('simultaneous stale-owner recovery admits only one successor',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'line-lock-stale-'));try{
  const child=spawn(process.execPath,['-e','process.exit(0)'],{stdio:'ignore',env:{}});const pid=child.pid;await new Promise((resolve,reject)=>{child.on('close',resolve);child.on('error',reject)});
  await writeFile(join(directory,'run.lock'),JSON.stringify({pid}),{mode:0o600});
  const results=await Promise.allSettled([acquireProfileLock(directory),acquireProfileLock(directory)]);const acquired=results.filter(r=>r.status==='fulfilled');assert.equal(acquired.length,1);await acquired[0].value();
 }finally{await rm(directory,{recursive:true,force:true})}
});
