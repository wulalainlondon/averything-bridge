import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
function run(input){return new Promise((resolve,reject)=>{
 const child=spawn(process.execPath,[new URL('./worker.mjs',import.meta.url).pathname],{env:{...process.env,LINEJS_ALLOW_CALLS:'0',LINEJS_DATA_DIR:'/nonexistent-credential-test'},stdio:['pipe','pipe','ignore']});
 let text='';const timeout=setTimeout(()=>{child.kill('SIGKILL');reject(new Error('worker_did_not_exit'))},3000);
 child.stdout.on('data',part=>{text+=part});child.on('error',reject);child.on('close',code=>{clearTimeout(timeout);resolve({code,events:text.trim().split('\n').filter(Boolean).map(JSON.parse)})});child.stdin.end(input);
})}
test('worker exits on owner pipe EOF without loading credentials',async()=>{
 const result=await run('');assert.equal(result.code,0);assert.ok(result.events.some(e=>e.state==='closed'));
});
test('disabled worker rejects dialing before credential or network use',async()=>{
 const result=await run(JSON.stringify({type:'start',callId:'line_'+'b'.repeat(64),peerName:'fixture'})+'\n');
 assert.equal(result.code,0);assert.ok(result.events.some(e=>e.code==='calling_not_enabled_or_duplicate'));assert.ok(!result.events.some(e=>e.state==='starting'||e.state==='ringing'));
});
