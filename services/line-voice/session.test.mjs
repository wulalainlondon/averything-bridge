import test from 'node:test';
import assert from 'node:assert/strict';
import {VoiceSession} from './session.mjs';
import {normalizer,linePayload,opusFromLine} from './audio.mjs';
import {opusCodecFactory} from '@evex/linejs/call';

const id='line_'+'a'.repeat(64);
function setup(){
 const data=new Map(),calls={invite:0,close:0,credentialsClosed:0},events=[];
 let finishReceive;const ended=new Promise(resolve=>{finishReceive=resolve});
 const transport={connect:async()=>{},inviteDetailed:async()=>{calls.invite++},waitForAnswerDetailed:async()=>({mediaReady:true}),close:async()=>{calls.close++;finishReceive()},send:async()=>{},async *receive(){await ended}};
 const runtime={base:{profile:{mid:'self'}},storage:{get:async k=>data.get(k),set:async(k,v)=>{data.set(k,v)}},close:async()=>{calls.credentialsClosed++}};
 const deps={emit:event=>events.push(event),credentials:async()=>runtime,resolvePeer:async()=>({mid:'peer'}),acquireRoute:async()=>({fakeCall:false}),transport:()=>transport,normalizer:async()=>({push:()=>[],close(){}}),linePayload,opusFromLine};
 return {deps,data,calls,events,runtime};
}
test('durable call ID survives finished call and prevents a second invitation',async()=>{
 const f=setup(),one=new VoiceSession(f.deps);await one.start({callId:id,peerName:'test'});await one.dispose();
 const two=new VoiceSession(f.deps);await assert.rejects(two.start({callId:id,peerName:'test'}),/already_used/);await two.dispose();assert.equal(f.calls.invite,1);
});
test('cancel during credential loading cannot ring after credentials arrive',async()=>{
 const f=setup();let resume;f.deps.credentials=()=>new Promise(resolve=>{resume=resolve});
 const call=new VoiceSession(f.deps),start=call.start({callId:id,peerName:'test'});await call.stop();resume(f.runtime);
 await assert.rejects(start,/call_cancelled/);await call.dispose();assert.equal(f.calls.invite,0);assert.equal(f.calls.credentialsClosed,1);
});
test('ambiguous invite outcome is never retried with the same durable ID',async()=>{
 const f=setup(),transport=f.deps.transport();transport.inviteDetailed=async()=>{f.calls.invite++;throw new Error('lost_ack')};
 const one=new VoiceSession(f.deps);await assert.rejects(one.start({callId:id,peerName:'test'}));await one.dispose();
 const two=new VoiceSession(f.deps);await assert.rejects(two.start({callId:id,peerName:'test'}),/already_used/);await two.dispose();assert.equal(f.calls.invite,1);
});
test('real Opus normalizer combines 10ms frames and preserves fixed 20ms output',async()=>{
 const codec=await opusCodecFactory(),enc=codec.newEncoder({sampleRate:48000,channels:1,frameDurationMs:10}),norm=await normalizer(),dec=codec.newDecoder({sampleRate:48000,channels:1});
 try{
  const packet=enc.encode({sampleRate:48000,channels:1,samples:Int16Array.from({length:480},(_,i)=>Math.round(5000*Math.sin(i/10)))});
  assert.equal(norm.push(packet).length,0);const packets=norm.push(packet);assert.equal(packets.length,1);assert.equal(dec.decode(packets[0]).samples.length,960);
  assert.deepEqual(opusFromLine(linePayload(packets[0])),packets[0]);assert.throws(()=>opusFromLine(Uint8Array.of(32,1)),/unsupported/);
 }finally{enc.close();norm.close();dec.close();}
});
test('audio burst is bounded and paced instead of being sent faster than playback',async()=>{
 const f=setup(),sent=[];f.deps.normalizer=async()=>({push:p=>[p],close(){}});f.deps.transport().send=async()=>{sent.push(performance.now())};
 const call=new VoiceSession(f.deps);await call.start({callId:id,peerName:'test'});
 for(let i=0;i<30;i++)call.audio(Uint8Array.of(1));assert.ok(call.queue.length<=8);
 await new Promise(resolve=>setTimeout(resolve,65));await call.dispose();
 assert.ok(sent.length>=2&&sent.length<=5);for(let i=1;i<sent.length;i++)assert.ok(sent[i]-sent[i-1]>=15);
 const n=sent.length;await new Promise(resolve=>setTimeout(resolve,30));assert.equal(sent.length,n);
});
test('unsupported wrapper and malformed Opus do not terminate reception of subsequent valid audio',async()=>{
 const f=setup(),transport=f.deps.transport(),originalReceive=transport.receive.bind(transport);
 const codec=await opusCodecFactory(),encoder=codec.newEncoder({sampleRate:48000,channels:1,frameDurationMs:20});
 const packet=encoder.encode({samples:Int16Array.from({length:960},(_,i)=>Math.round(5000*Math.sin(i/10))),sampleRate:48000,channels:1});
 const malformed=new Uint8Array(71);malformed.set([0,0x7a,107]);
 transport.receive=async function*(){yield linePayload(packet);yield Uint8Array.of(32,1);yield malformed;yield linePayload(packet);yield*originalReceive();};
 f.deps.normalizer=normalizer;
 const call=new VoiceSession(f.deps);
 try{
  await call.start({callId:id,peerName:'test'});
  for(let i=0;i<20&&f.events.filter(e=>e.type==='audio').length<2;i++)await new Promise(resolve=>setTimeout(resolve,5));
  assert.equal(f.events.filter(e=>e.type==='audio').length,2);
  assert.deepEqual(f.events.filter(e=>e.code==='line_audio_packet_dropped').map(e=>e.stage),['wrapper','decode']);
  assert.equal(call.state,'connected');assert.equal(f.calls.close,0);
  assert.equal(f.events.some(e=>e.type==='error'),false);
 }finally{encoder.close();await call.dispose();}
});
test('native prefix transition from 00 to 10 preserves sustained non-silent Opus reception',async()=>{
 const f=setup(),transport=f.deps.transport(),originalReceive=transport.receive.bind(transport);
 const codec=await opusCodecFactory(),encoder=codec.newEncoder({sampleRate:48000,channels:1,frameDurationMs:20,signal:'voice'});
 const decoder=codec.newDecoder({sampleRate:48000,channels:1}),payloads=[];
 for(let frame=0;frame<30;frame++){
  const packet=encoder.encode({samples:Int16Array.from({length:960},(_,i)=>Math.round(6000*Math.sin(2*Math.PI*440*(i+frame*960)/48000))),sampleRate:48000,channels:1});
  const payload=linePayload(packet);payload[0]=frame<2?0:0x10;payloads.push(payload);
 }
 transport.receive=async function*(){yield*payloads;yield*originalReceive();};f.deps.normalizer=normalizer;
 const call=new VoiceSession(f.deps);
 try{
  await call.start({callId:id,peerName:'test'});
  for(let i=0;i<40&&f.events.filter(e=>e.type==='audio').length<30;i++)await new Promise(resolve=>setTimeout(resolve,5));
  const audio=f.events.filter(e=>e.type==='audio');assert.equal(audio.length,30);
  const samples=audio.flatMap(e=>Array.from(decoder.decode(new Uint8Array(Buffer.from(e.data,'base64'))).samples));
  const rms=Math.sqrt(samples.reduce((sum,value)=>sum+value*value,0)/samples.length);
  assert.ok(rms>1000,'synthetic tone must remain audible after decode and re-encode');
  assert.equal(f.events.some(e=>e.code==='line_audio_packet_dropped'||e.type==='error'),false);assert.equal(call.state,'connected');
 }finally{encoder.close();decoder.close();await call.dispose();}
});
test('remote release records numeric codes without claiming manual hangup or exposing raw fields',async()=>{
 const f=setup(),transport=f.deps.transport();let debug;
 f.deps.transport=callback=>{debug=callback;return transport;};
 const call=new VoiceSession(f.deps);await call.start({callId:id,peerName:'test'});
 debug({type:'rel_req',relCode:7,commMediaFlags:1,relPhrase:'private phrase',releaser:'private identifier',userRelCode:new Uint8Array([1,2])});
 await call.dispose();
 assert.equal(f.data.get('bridge.lineVoice.'+id).reason,'remote_release');
 assert.deepEqual(f.events.find(e=>e.code==='line_remote_release'),{type:'diagnostic',code:'line_remote_release',relCode:7,commMediaFlags:1});
});
test('invite acknowledgement failure does not claim ringing and records a distinct safe timeout',async()=>{
 const f=setup();f.deps.transport().inviteDetailed=async()=>{f.calls.invite++;throw new Error('PLANET reply timeout');};
 const call=new VoiceSession(f.deps);
 try{
  await assert.rejects(call.start({callId:id,peerName:'test'}),/line_invite_timeout/);
  assert.equal(f.events.some(e=>e.state==='ringing'),false);
  const failed=f.events.find(e=>e.code==='line_call_stage'&&e.status==='failed');
  assert.equal(failed.stage,'invite');assert.equal(failed.errorCode,'line_invite_timeout');assert.ok(failed.elapsedMs>=0);
 }finally{await call.dispose();}
});
test('answer timeout is distinguished from invite timeout without exposing raw SDK errors',async()=>{
 const f=setup();f.deps.transport().waitForAnswerDetailed=async()=>{throw new Error('PLANET reply timeout');};
 const call=new VoiceSession(f.deps);
 try{
  await assert.rejects(call.start({callId:id,peerName:'test'}),/line_answer_timeout/);
  assert.equal(f.events.filter(e=>e.state==='ringing').length,1);
  assert.equal(f.events.find(e=>e.status==='failed').stage,'answer');
 }finally{await call.dispose();}
 const g=setup();g.deps.transport().inviteDetailed=async()=>{throw new Error('Authorization: private fixture key');};
 const second=new VoiceSession(g.deps);
 try{
  await assert.rejects(second.start({callId:id,peerName:'test'}),/^Error: line_invite_failed$/);
  assert.equal(JSON.stringify(g.events).includes('private fixture key'),false);
 }finally{await second.dispose();}
});
