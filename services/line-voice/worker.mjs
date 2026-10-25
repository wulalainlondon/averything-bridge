import {Client} from '@evex/linejs';
import {loadPatchedTransport} from './linejs-transport.mjs';
import {credentials,resolvePeer} from './credentials.mjs';
import {normalizer,linePayload,opusFromLine} from './audio.mjs';
import {VoiceSession} from './session.mjs';
console.log=()=>{};console.error=()=>{};
let PlanetTransport,patchInfo;
try{({PlanetTransport,patchInfo}=await loadPatchedTransport());}
catch{process.stdout.write(JSON.stringify({type:'error',code:'line_transport_patch_invalid'})+'\n');process.exit(1);}
let blocked=false,outputDead=false,started=false,finishing=false,buffer='',timer;
process.stdout.on('drain',()=>{blocked=false;});
process.stdout.on('error',()=>{outputDead=true;void finish();});
function emit(value){if(outputDead||(blocked&&value.type==='audio'))return;blocked=!process.stdout.write(JSON.stringify(value)+'\n');}
const session=new VoiceSession({credentials,resolvePeer,normalizer,linePayload,opusFromLine,emit,
  acquireRoute:(base,to)=>new Client(base).call.acquireRoute({to,callType:'AUDIO'}),
  transport:debug=>new PlanetTransport({localMid:session.runtime.base.profile.mid,mediaKeyMode:'audio-reverse-stage',timeoutMs:10000,debug})});
let startTask;
async function finish(){
  if(finishing)return;finishing=true;clearTimeout(timer);
  await session.stop().catch(()=>{});
  await startTask?.catch(()=>{});await session.dispose().catch(()=>{});
  process.stdin.pause();process.exitCode=0;
  // Broken signaling must not leave sockets or a late setup alive indefinitely.
  setTimeout(()=>process.exit(),100).unref();
}
function command(value){
  if(value.type==='stop'){void finish();return;}
  if(value.type==='start'){
    if(started||process.env.LINEJS_ALLOW_CALLS!=='1'){emit({type:'error',code:'calling_not_enabled_or_duplicate'});void finish();return;}
    started=true;
    emit({type:'diagnostic',code:'line_transport_patch',...patchInfo});
    timer=setTimeout(()=>void finish(),30*60*1000);
    startTask=session.start(value).catch(error=>{
      if(!session.stopped)emit({type:'error',code:/^[a-z_]{1,64}$/.test(error?.message??'')?error.message:'line_call_failed'});
    }).finally(()=>{if(session.state!=='connected')void finish();});
    return;
  }
  if(value.type==='audio'&&typeof value.data==='string'&&value.data.length<=5400&&/^[A-Za-z0-9+/]*={0,2}$/.test(value.data))session.audio(new Uint8Array(Buffer.from(value.data,'base64')));
}
process.stdin.setEncoding('utf8');
process.stdin.on('data',chunk=>{
  buffer+=chunk;if(buffer.length>65536){emit({type:'error',code:'ipc_input_limit'});void finish();return;}
  let newline;while((newline=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,newline);buffer=buffer.slice(newline+1);try{command(JSON.parse(line));}catch{emit({type:'error',code:'ipc_invalid'});void finish();}}
});
process.stdin.on('end',()=>void finish());
process.stdin.on('error',()=>void finish());
process.on('SIGTERM',()=>void finish());process.on('SIGINT',()=>void finish());
process.on('uncaughtException',()=>{emit({type:'error',code:'worker_failed'});void finish();});
process.on('unhandledRejection',()=>{emit({type:'error',code:'worker_failed'});void finish();});
