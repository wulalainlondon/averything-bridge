// One call, one private stdin stream. Permanent account credentials and SDK
// login helpers are intentionally absent from this worker.
import {loadPatchedTransport} from './linejs-transport.mjs';
import {normalizer,linePayload,opusFromLine} from './audio.mjs';
import {VoiceSession} from './session.mjs';
console.log=()=>{};console.error=()=>{};
let PlanetTransport;
try{({PlanetTransport}=await loadPatchedTransport())}catch{process.stdout.write(JSON.stringify({type:'error',code:'line_transport_patch_invalid'})+'\n');process.exit(1)}
let session,started=false,finishing=false,buffer='',blocked=false,setupTimer,holdTimer,keepalive,statsTimer,activeAt=0,lastAudio=0;
process.stdout.on('drain',()=>{blocked=false});
function emit(value){if(blocked&&value.type==='audio')return;blocked=!process.stdout.write(JSON.stringify(value)+'\n');if(value.type==='state'&&value.state==='connected'){clearTimeout(setupTimer);activeAt=performance.now();statsTimer=setInterval(()=>emit({type:'diagnostic',code:'line_media_counts',elapsedMs:Math.round(performance.now()-activeAt),txFrames:session?.sentFrames??0,rxFrames:session?.receivedFrames??0}),60000);holdTimer=setTimeout(()=>{emit({type:'diagnostic',code:'line_call_duration_limit',elapsedMs:Math.round(performance.now()-activeAt)});void finish('duration_limit')},1200000);keepalive=setInterval(()=>{if(Date.now()-lastAudio>60)session?.audio(Buffer.from([0xf8,0xff,0xfe]))},20)}if(value.type==='state'&&value.state==='closed')void finish();}
let task;
async function finish(reason='hangup'){
 if(finishing)return;finishing=true;clearTimeout(setupTimer);clearTimeout(holdTimer);clearInterval(keepalive);clearInterval(statsTimer);
 await session?.stop(reason).catch(()=>{});await task?.catch(()=>{});await session?.dispose().catch(()=>{});
 process.stdin.pause();process.exitCode=0;setTimeout(()=>process.exit(),100).unref();
}
async function start(value){
 const material=value.material;
 if(!material||!/^u[a-f0-9]{32}$/.test(material.principal??'')||!/^u[a-f0-9]{32}$/.test(material.target??'')||!material.route||material.target===material.principal)throw new Error('line_call_material_invalid');
 const state=new Map();
 session=new VoiceSession({credentials:async()=>({base:{profile:{mid:material.principal}},storage:{get:async key=>state.get(key),set:async(key,v)=>{state.set(key,v)}},close:async()=>{state.clear()}}),resolvePeer:async()=>({mid:material.target}),normalizer,linePayload,opusFromLine,emit,acquireRoute:async()=>material.route,transport:debug=>new PlanetTransport({localMid:material.principal,mediaKeyMode:'audio-reverse-stage',timeoutMs:10000,debug})});
 setupTimer=setTimeout(()=>void finish('setup_timeout'),60000);
 await session.start({callId:value.callId,peerName:value.peerName});
}
function command(value){
 if(value.type==='stop'){void finish();return}
 if(value.type==='start'){
  if(started||process.env.LINEJS_ALLOW_CALLS!=='1'){emit({type:'error',code:'line_call_not_enabled_or_duplicate'});void finish();return}
  started=true;task=start(value).catch(()=>{emit({type:'error',code:'line_call_failed'})}).finally(()=>{if(session?.state!=='connected')void finish()});return;
 }
 if(value.type==='audio'&&session&&typeof value.data==='string'&&value.data.length<=6000){lastAudio=Date.now();session.audio(Buffer.from(value.data,'base64'))}
}
process.stdin.setEncoding('utf8');process.stdin.on('data',chunk=>{
 buffer+=chunk;if(buffer.length>131072){void finish('input_limit');return}
 let at;while((at=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,at);buffer=buffer.slice(at+1);try{command(JSON.parse(line))}catch{void finish('invalid_input')}}
});
process.stdin.on('end',()=>void finish());process.stdout.on('error',()=>void finish());process.on('SIGTERM',()=>void finish());process.on('SIGINT',()=>void finish());

emit({type:'ready',protocol_version:2});
