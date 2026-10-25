// Synthetic encrypted packets only: no credentials, LINE routes or sockets.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {sdkCallURL,loadPatchedTransport} from './linejs-transport.mjs';
import {patchSrtpSource} from './srtp-rollover.mjs';
const original=await import(new URL('./srtp.js',sdkCallURL));
const {srtp:fixed}=await loadPatchedTransport();
const key=new Uint8Array(30).fill(7),ssrc=123;
const packet=(seq,source=ssrc)=>original.buildRtp({payloadType:111,seq,timestamp:(seq*960)>>>0,ssrc:source,payload:Uint8Array.of(1,2,3,4)});
// Encrypt in send order: independent upstream sender updates its ROC at65535.
async function orderedEncrypted(sequence,source=ssrc){const tx=await original.deriveSrtpContext(key),out=[];for(const seq of sequence){const plain=packet(seq,source);out.push({plain,wire:await original.srtpEncrypt(tx,plain)})}return out;}

test('baseline receiver reproduces permanent authentication failure after sequence wrap',async()=>{
 const frames=await orderedEncrypted([65534,65535,0,1]),rx=await original.deriveSrtpContext(key);
 for(const f of frames.slice(0,2))await original.srtpDecrypt(rx,f.wire);
 for(const f of frames.slice(2))await assert.rejects(original.srtpDecrypt(rx,f.wire),/auth tag mismatch/);
 assert.equal(rx.rocs.get(ssrc)??0,0);
});
test('corrected receiver interoperates with unchanged upstream encryption across wrap',async()=>{
 const frames=await orderedEncrypted([65534,65535,0,1]),rx=await original.deriveSrtpContext(key);
 for(const f of frames)assert.deepEqual(await fixed.srtpDecrypt(rx,f.wire),f.plain);
 assert.equal(rx.rocs.get(ssrc),1);
});
test('lost last pre-wrap packet and reordered old-epoch packet do not desynchronize receiver',async()=>{
 const f=await orderedEncrypted([65534,65535,0,1]),rx=await original.deriveSrtpContext(key);
 for(const index of [0,2,1,3])assert.deepEqual(await fixed.srtpDecrypt(rx,f[index].wire),f[index].plain);
 assert.equal(rx.rocs.get(ssrc),1);
 await assert.rejects(fixed.srtpDecrypt(rx,f[1].wire),/replay/);
});
test('tampered next-epoch packet cannot poison ROC or consume the valid packet',async()=>{
 const f=await orderedEncrypted([65534,65535,0,1]),rx=await original.deriveSrtpContext(key);await fixed.srtpDecrypt(rx,f[0].wire);
 const bad=f[2].wire.slice();bad[bad.length-1]^=1;await assert.rejects(fixed.srtpDecrypt(rx,bad),/auth tag mismatch/);assert.equal(rx.rocs.get(ssrc),0);
 for(const index of [1,2,3])assert.deepEqual(await fixed.srtpDecrypt(rx,f[index].wire),f[index].plain);
});
test('concurrent callers serialize receiver state and recover after an authentication error',async()=>{
 const f=await orderedEncrypted([65534,65535,0,1]),rx=await original.deriveSrtpContext(key);
 const values=await Promise.all(f.map(v=>fixed.srtpDecrypt(rx,v.wire)));assert.deepEqual(values,f.map(v=>v.plain));
 const nextTx=await original.deriveSrtpContext(key);nextTx.rocs.set(ssrc,1);const plain=packet(2),wire=await original.srtpEncrypt(nextTx,plain),bad=wire.slice();bad[bad.length-1]^=1;
 const results=await Promise.allSettled([fixed.srtpDecrypt(rx,bad),fixed.srtpDecrypt(rx,wire)]);assert.equal(results[0].status,'rejected');assert.deepEqual(results[1].value,plain);
});
test('SSRC streams have independent ROC and authenticated replay windows',async()=>{
 const first=await orderedEncrypted([65535,0]),second=await orderedEncrypted([10,11],456),rx=await original.deriveSrtpContext(key);
 for(const f of [first[0],second[0],first[1],second[1]])assert.deepEqual(await fixed.srtpDecrypt(rx,f.wire),f.plain);
 assert.equal(rx.rocs.get(ssrc),1);assert.equal(rx.rocs.get(456),0);
 const tx=await original.deriveSrtpContext(key);await assert.rejects(fixed.srtpDecrypt(rx,await original.srtpEncrypt(tx,packet(10,456))),/replay/);
});
test('receiver accepts limited reordering and rejects packets outside its replay window',async()=>{
 const f=await orderedEncrypted(Array.from({length:81},(_,i)=>i)),rx=await original.deriveSrtpContext(key);
 for(const index of [0,80,78])assert.deepEqual(await fixed.srtpDecrypt(rx,f[index].wire),f[index].plain);
 await assert.rejects(fixed.srtpDecrypt(rx,f[1].wire),/replay/);
});
test('accelerated long stream crosses two sequence wraps without losing decryptability',async()=>{
 const tx=await original.deriveSrtpContext(key),rx=await original.deriveSrtpContext(key);
 for(let index=0;index<65544;index++){const plain=packet((65532+index)&65535),wire=await original.srtpEncrypt(tx,plain);assert.deepEqual(await fixed.srtpDecrypt(rx,wire),plain)}
 assert.equal(rx.rocs.get(ssrc),2);
});
test('changed SRTP source refuses the patch rather than silently using an unknown implementation',async()=>{
 const source=await readFile(new URL('./srtp.js',sdkCallURL),'utf8');assert.throws(()=>patchSrtpSource(source+'\n'),/baseline_mismatch/);assert.throws(()=>patchSrtpSource(patchSrtpSource(source)),/baseline_mismatch/);
});

test('unauthenticated streams cannot consume receive capacity',async()=>{
 const tx=await original.deriveSrtpContext(key),rx=await original.deriveSrtpContext(key);
 for(let source=1;source<=127;source++)await fixed.srtpDecrypt(rx,await original.srtpEncrypt(tx,packet(1,source)));
 const last=await original.srtpEncrypt(tx,packet(1,128)),bad=last.slice();bad[bad.length-1]^=1;await assert.rejects(fixed.srtpDecrypt(rx,bad),/auth tag mismatch/);
 await fixed.srtpDecrypt(rx,last);await assert.rejects(fixed.srtpDecrypt(rx,await original.srtpEncrypt(tx,packet(1,129))),/stream limit/);
});

test('decrypt queue is bounded and a refused request can be retried after capacity returns',async()=>{
 const frames=await orderedEncrypted(Array.from({length:65},(_,i)=>i)),rx=await original.deriveSrtpContext(key);
 const results=await Promise.allSettled(frames.map(f=>fixed.srtpDecrypt(rx,f.wire)));assert.equal(results.filter(r=>r.status==='fulfilled').length,64);assert.match(results[64].reason.message,/queue limit/);
 assert.deepEqual(await fixed.srtpDecrypt(rx,frames[64].wire),frames[64].plain);
});
