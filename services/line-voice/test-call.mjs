// Explicit bounded live test. A stable --id is mandatory; rerunning it cannot redial.
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {opusCodecFactory} from '@evex/linejs/call';
import {liveTestOptions} from './live-test-options.mjs';
import {testAudioSource} from './test-audio.mjs';
const values=liveTestOptions(process.argv.slice(2));
const callId='line_'+createHash('sha256').update(values.id).digest('hex');
let buffer='',connected=false,toneSentFrames=0,silenceSentFrames=0,encoder,decoder,tone,hold,metrics,forceKill,connectedAt,finishing=false,stopReason;
const codec=await opusCodecFactory();encoder=codec.newEncoder({sampleRate:48000,channels:1,frameDurationMs:20,signal:'voice'});decoder=codec.newDecoder({sampleRate:48000,channels:1});
const source=testAudioSource(encoder,values.toneSeconds);
const child=spawn(process.execPath,[new URL('./worker.mjs',import.meta.url).pathname],{stdio:['pipe','pipe','ignore'],env:{...process.env,LINEJS_ALLOW_CALLS:'1'}});
const finish=(reason='test_stopped')=>{
 if(finishing)return;finishing=true;stopReason=reason;clearInterval(tone);clearInterval(metrics);clearTimeout(hold);clearTimeout(timeout);
 if(child.stdin.writable)child.stdin.end(JSON.stringify({type:'stop'})+'\n');
 forceKill=setTimeout(()=>child.kill('SIGKILL'),5000);
};
const timeout=setTimeout(()=>finish('setup_timeout'),60000);
let received=0,nonSilent=0;
const stats=()=>({holdRequestedSeconds:values.holdSeconds,connectedElapsedSeconds:connectedAt?(performance.now()-connectedAt)/1000:0,toneRequestedSeconds:values.toneSeconds,toneSentFrames,toneEncodedSeconds:toneSentFrames*0.02,silenceSentFrames,outgoingEncodedSeconds:(toneSentFrames+silenceSentFrames)*0.02,receivedAudioFrames:received,nonSilentFrames:nonSilent});
child.stdout.on('data',data=>{
 buffer+=data;let next;
 while((next=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,next);buffer=buffer.slice(next+1);const f=JSON.parse(line);
  if(f.type==='audio'){received++;const pcm=decoder.decode(new Uint8Array(Buffer.from(f.data,'base64'))).samples;if(pcm.some(v=>Math.abs(v)>100))nonSilent++;continue;}
  process.stdout.write(JSON.stringify(f)+'\n');
  if(f.state==='connected'&&!connected&&!finishing){connected=true;connectedAt=performance.now();clearTimeout(timeout);hold=setTimeout(()=>finish('hold_complete'),values.holdSeconds*1000);
   metrics=setInterval(()=>process.stdout.write(JSON.stringify({liveTestProgress:true,...stats()})+'\n'),60000);
   tone=setInterval(()=>{
   const {tone:audible,packet:p}=source.next();
   if(p&&child.stdin.writable){child.stdin.write(JSON.stringify({type:'audio',data:Buffer.from(p).toString('base64')})+'\n');if(audible)toneSentFrames++;else silenceSentFrames++;}
  },20);}
  if(f.type==='error'||f.state==='closed')finish(f.type==='error'?f.code:'worker_closed');
 }
});
child.on('close',(code,signal)=>{clearTimeout(timeout);clearTimeout(hold);clearTimeout(forceKill);clearInterval(tone);clearInterval(metrics);encoder.close();decoder.close();process.stdout.write(JSON.stringify({liveTestEnded:true,connected,...stats(),stopReason:stopReason??'worker_exited',signal,recipientAudibilityStillRequiresHumanConfirmation:true,exitCode:code})+'\n')});
child.on('error',()=>finish('worker_spawn_failed'));
child.stdin.on('error',()=>{});
process.on('SIGINT',()=>finish('operator_stopped'));process.on('SIGTERM',()=>finish('operator_stopped'));
child.stdin.write(JSON.stringify({type:'start',callId,peerName:values.peer})+'\n');
