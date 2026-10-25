import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdir,stat,mkdtemp,rm} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {BaseClient} from '@evex/linejs/base';
import type {BaseStorage} from '@evex/linejs/storage';
import {Agent} from 'undici';
import QRCode from 'qrcode';
import {commitStagedLogin} from './pairing-commit.js';
import {LineFailure} from './types.js';

interface Storage extends BaseStorage {load():Promise<Storage>;getAll():Promise<Record<string,unknown>>;mutate(op:(data:Record<string,unknown>)=>void):Promise<void>}
interface PairState {state:string;qr_data_url?:string;pin?:string;expires_at:number;principal?:string;name?:string;code?:string}
export class Pairing {
 private committing?:{principal:string;task:Promise<PairState>};
 private pending?:{principal:string;commit:(expected:string)=>Promise<void>;cleanup:()=>Promise<void>};
 private state:PairState={state:'idle',expires_at:0};private task?:Promise<void>;private abort?:AbortController;
 snapshot(){return {...this.state}}
 start(reauth=false){
  if(this.task||this.pending||this.committing)throw new LineFailure('line_pairing_already_started');
  this.state={state:'pairing',expires_at:Date.now()+8*60*1000};this.abort=new AbortController();
  this.task=this.run(this.abort.signal,reauth).catch(error=>{this.state={state:'failed',expires_at:0,code:error instanceof LineFailure?error.code:'line_pairing_failed'}}).finally(()=>{this.task=undefined});
  return this.snapshot();
 }
 async close(){this.abort?.abort();await this.task;await this.committing?.task.catch(()=>{});await this.discard()}
 async discard(){const pending=this.pending;this.pending=undefined;await pending?.cleanup();if(this.state.state!=='paired')this.state={state:'failed',expires_at:0,code:'line_pairing_cancelled'}}
 async commit(expected:string){
  if(this.committing){if(this.committing.principal!==expected)throw new LineFailure('line_principal_mismatch');return this.committing.task}
  if(this.state.state==='paired'&&this.state.principal===expected)return this.snapshot();
  const pending=this.pending;if(!pending||pending.principal!==expected)throw new LineFailure('line_principal_mismatch');this.pending=undefined;const name=this.state.name;
  const task=(async()=>{try{await pending.commit(expected);this.state={state:'paired',expires_at:0,principal:pending.principal,name};return this.snapshot()}finally{await pending.cleanup();this.committing=undefined}})();
  this.committing={principal:pending.principal,task};return task;
 }

 private async run(signal:AbortSignal,reauth:boolean){
  const directory=resolve(process.env.LINEJS_DATA_DIR??''),helper=process.env.LINEJS_KEYCHAIN_HELPER,shared=process.env.LINEJS_SHARED_DIR;
  if(!process.env.LINEJS_DATA_DIR||!helper||!shared)throw new LineFailure('line_pairing_configuration_invalid');
  await mkdir(directory,{recursive:true,mode:0o700});
  let releaseLock:(()=>Promise<void>)|undefined,key:Buffer|undefined,agent:Agent|undefined;let saves=Promise.resolve(),staging:string|undefined,commitTimer:ReturnType<typeof setTimeout>|undefined;
  let retained=false,cleaned=false;const cleanup=async()=>{if(cleaned)return;cleaned=true;clearTimeout(commitTimer);await saves.catch(()=>{});await agent?.destroy();key?.fill(0);if(staging)await rm(staging,{recursive:true,force:true});await releaseLock?.()};
  const timer=setTimeout(()=>this.abort?.abort(),8*60*1000);
  try{
   const locks=await import(pathToFileURL(join(shared,'profile-lock.mjs')).href) as {acquireProfileLock(dir:string):Promise<()=>Promise<void>>};
   try{releaseLock=await locks.acquireProfileLock(directory)}catch{throw new LineFailure('line_credential_store_busy')}
   const exists=await stat(join(directory,'session.enc')).then(()=>true,e=>{if(e.code==='ENOENT')return false;throw e});
   const service='tw.wulala.linejs-lab.'+createHash('sha256').update(directory).digest('hex').slice(0,20);
   key=await new Promise<Buffer>((resolveKey,reject)=>{
    const child=spawn(helper,[exists?'get':'create',service],{stdio:['ignore','pipe','ignore']});let data='';const timeout=setTimeout(()=>child.kill(),10000);
    child.stdout.on('data',chunk=>{data+=chunk;if(data.length>100)child.kill()});child.on('error',()=>{clearTimeout(timeout);reject(new LineFailure('line_keychain_unavailable'))});child.on('close',code=>{clearTimeout(timeout);const bytes=Buffer.from(data,'base64');if(code!==0||bytes.length!==32)reject(new LineFailure('line_keychain_unavailable'));else resolveKey(bytes)});
   });
   const module=await import(pathToFileURL(join(shared,'protected-storage.mjs')).href) as {ProtectedStorage:new(directory:string,key:Buffer)=>Storage};
   const previous=await new module.ProtectedStorage(directory,key).load();if(await previous.get('lab.authToken')&&!reauth)throw new LineFailure('line_already_paired');
   staging=await mkdtemp(join(directory,'pair-stage-'));const storage=await new module.ProtectedStorage(staging,key).load();
   agent=new Agent({allowH2:false,connectTimeout:15000,maxResponseSize:9*1024*1024});
   const dispatcher=agent;
   const client=new BaseClient({device:'ANDROIDSECONDARY',storage,fetch:request=>fetch(request,{dispatcher,signal:AbortSignal.any([request.signal,signal,AbortSignal.timeout(30000)])} as RequestInit)});
   client.on('update:authtoken',token=>{client.authToken=token;saves=saves.then(()=>storage.set('lab.authToken',token));});
   client.on('qrcall',url=>{void QRCode.toDataURL(url,{width:320,margin:2}).then(qr=>{if(!signal.aborted&&this.state.state==='pairing')this.state.qr_data_url=qr}).catch(()=>{});});
   client.on('pincall',pin=>{if(!signal.aborted)this.state.pin=String(pin)});
   await client.loginProcess.login({qr:true,v3:true});await saves;
   if(signal.aborted||!client.profile?.mid)throw new LineFailure('line_pairing_interrupted');
   const keys=Object.keys(await storage.getAll()).filter(k=>k.startsWith('e2eeKeys:'));if(!keys.length)throw new LineFailure('line_pairing_e2ee_missing');
   await agent.destroy();agent=undefined;
   const principal=client.profile.mid,stage=staging;
   this.pending={principal,cleanup,commit:async expected=>{
    clearTimeout(commitTimer);if(signal.aborted)throw new LineFailure('line_pairing_interrupted');
    if(expected!==principal)throw new LineFailure('line_principal_mismatch');
    const old=await previous.getAll();await storage.mutate(data=>{for(const [name,value]of Object.entries(old))if(name.startsWith('e2eeKeys:')&&!Object.hasOwn(data,name))data[name]=value});
    await commitStagedLogin(directory,stage,principal,expected);
   }};
   retained=true;this.state={state:'awaiting_commit',expires_at:Date.now()+30000,principal,name:client.profile.displayName};
   commitTimer=setTimeout(()=>void this.discard(),30000);
  }finally{
   clearTimeout(timer);if(!retained)await cleanup();
  }
 }
}
