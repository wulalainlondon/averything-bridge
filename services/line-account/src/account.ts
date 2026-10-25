import {createHash} from 'node:crypto';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {LineFailure,object,text,integer,type Base,type Runtime,type RawMessage,type WireMessage,type ServiceEvent} from './types.js';
import {sendResource} from './media.js';
import {findRooms,roomEvents} from './openchat.js';
import {messagePage} from './history.js';
import {Pairing} from './pairing.js';

export type RuntimeLoader=()=>Promise<Runtime>;
export class AccountService {
 private runtime?:Runtime;
 private opening?:Promise<Runtime>;
 private closed=false;
 private pairingRequested=false;
 private directory?:{at:number;contacts:{id:string;name:string;alias:string;status_message:string;has_avatar:boolean;kind:string}[]};
 private sends=new Map<string,{fingerprint:string;result:Promise<{message_id:string}>}>();
 private pairing=new Pairing();
 constructor(private loader:RuntimeLoader){}
 private async ready():Promise<Runtime>{
  if(this.closed)throw new LineFailure('line_account_closed');
  if(this.pairingRequested&&this.pairing.snapshot().state==='failed')this.pairingRequested=false;
  if(this.pairingRequested)throw new LineFailure('line_pairing_account_busy');
  if(this.runtime)return this.runtime;
  this.opening??=this.loader().then(async runtime=>{if(this.closed){await runtime.close();throw new LineFailure('line_account_closed')};return this.runtime=runtime;}).finally(()=>{this.opening=undefined});
  return this.opening;
 }
 async close(){this.closed=true;await this.pairing.close();await this.opening?.catch(()=>{});await this.runtime?.close();this.runtime=undefined;}
 private message(raw:RawMessage,base:Base,target:string):WireMessage {
  return {message_ref:String(raw.id??''),target_ref:target,direction:raw.from===base.profile.mid?'outbound':'inbound',...(typeof raw.text==='string'?{text:raw.text.slice(0,5000)}:{}),content_type:String(raw.contentType??'UNKNOWN'),timestamp:Number(raw.createdTime??0)};
 }
 async invoke(method:string,input:unknown):Promise<unknown>{
  if(method==='close'){await this.close();return {closed:true}}
  if(this.closed)throw new LineFailure('line_account_closed');
  if(method==='pair'){
   const reauth=object(input??{}).reauthenticate===true;
   if((this.runtime||this.opening)&&!reauth)throw new LineFailure('line_pairing_account_busy');
   this.pairingRequested=true;
   try{await this.opening?.catch(()=>{});await this.runtime?.close();this.runtime=undefined;return this.pairing.start(reauth)}catch(error){this.pairingRequested=false;throw error}
  }
  if(method==='pair_commit'){try{return await this.pairing.commit(text(object(input).principal,128))}finally{this.pairingRequested=false}}
  if(method==='pair_discard'){await this.pairing.discard();this.pairingRequested=false;return {discarded:true}}
  if(method==='pair_state')return this.pairing.snapshot();
  const runtime=await this.ready(),base=runtime.base;
  if(method==='resume')return {principal:base.profile.mid,name:base.profile.displayName??''};
  if(method==='health'){const profile=await base.talk.getProfile();if(profile.mid!==base.profile.mid)throw new LineFailure('line_principal_mismatch');return {principal:profile.mid,name:profile.displayName??''};}
  const args=object(input??{});
  if(method==='find_targets'){
   const query=typeof args.query==='string'?args.query.trim().normalize('NFKC').toLocaleLowerCase():'';
   const limit=integer(args.limit,1,50);
   if(!this.directory||Date.now()-this.directory.at>60000){
    const ids=(await base.relation.getUserFriendIds({request:{blockStatus:'ALL'}})).userFriendMids??[];
    if(ids.length>2000)throw new LineFailure('line_directory_limit');
    const contacts=[];
    for(let i=0;i<ids.length;i+=100){for(const c of await base.talk.getContacts({mids:ids.slice(i,i+100)}))contacts.push({id:c.mid,name:c.displayName,alias:c.displayNameOverridden??'',status_message:(c.statusMessage??'').slice(0,256),has_avatar:!!(c.picturePath||c.pictureStatus),kind:'user'});}
    this.directory={at:Date.now(),contacts};
   }
   return this.directory.contacts.filter(c=>[c.name,c.alias].some(n=>n.normalize('NFKC').toLocaleLowerCase().includes(query))).slice(0,limit);
  }
  if(method==='download_resource'){
   const target=text(args.target,128),id=text(args.message,128);
   const history=await base.talk.getRecentMessagesV2({messageBoxId:target,messagesCount:100});const raw=history.find(m=>String(m.id)===id);
   if(!raw||(raw.to!==target&&raw.from!==target))throw new LineFailure('line_resource_message_not_recent');
   const size=Number(raw.contentMetadata?.FILE_SIZE??0);if(!Number.isFinite(size)||size<=0||size>8*1024*1024+1024)throw new LineFailure('line_resource_size_invalid');
   const file=await base.obs.downloadMediaByE2EE(raw);if(!file||file.size>8*1024*1024)throw new LineFailure('line_resource_size_invalid');
   return {data:Buffer.from(await file.arrayBuffer()).toString('base64'),name:file.name,mime:file.type||'application/octet-stream'};
  }
  if(method==='find_openchats'){const query=typeof args.query==='string'?args.query.trim().normalize('NFKC').toLocaleLowerCase():'';return findRooms(runtime,query,integer(args.limit,1,20))}
  if(method==='poll_openchat'){
   const target=text(args.target,128),cursor=typeof args.cursor==='string'?args.cursor:'';const result=await roomEvents(base,target,cursor,100);
   return {events:result.messages.map(m=>({kind:m.kind,event_key:m.key,target,message:{...this.message(m.raw,base,target),direction:'unknown'}})),cursor:result.cursor,gap:!cursor};
  }
  if(method==='get_message_page')return messagePage(base,text(args.target,128),integer(args.limit,1,100),typeof args.cursor==='string'?args.cursor:'',raw=>this.message(raw,base,text(args.target,128)));
  if(method==='get_messages'){
   const target=text(args.target,128),limit=integer(args.limit,1,100);
   if(args.cursor)throw new LineFailure('line_history_pagination_unverified');
   if(args.kind==='openchat'){const result=await roomEvents(base,target,'',limit);return result.messages.map(m=>({...this.message(m.raw,base,target),direction:'unknown'}))}
   if(args.kind!=='user'&&args.kind!=='group')throw new LineFailure('line_target_kind_unavailable');
   const messages=await base.talk.getRecentMessagesV2({messageBoxId:target,messagesCount:limit});
   const out=[];
   for(const raw of messages){const decoded=await base.e2ee.decryptE2EEMessage(raw);out.push(this.message(decoded,base,target));}
   return out;
  }
  if(method==='send_message'){
   const id=text(args.operation_id,128),target=text(args.target,128),content=object(args.content),value=content.resource_ref?'':text(content.text);
   if(!/^lo_[a-f0-9]{32}$/.test(id)||!/^u[a-f0-9]{32}$/.test(target))throw new LineFailure('line_send_invalid');
   if(content.resource_ref&&(content.text||content.reply_to))throw new LineFailure('line_resource_caption_unavailable');
   const fingerprint=createHash('sha256').update(JSON.stringify([target,content])).digest('hex');
   const prior=this.sends.get(id);if(prior){if(prior.fingerprint!==fingerprint)throw new LineFailure('line_intent_conflict');return prior.result;}
   if(this.sends.size>=10000)throw new LineFailure('line_operation_cache_full');
   const result=(async()=>{
    if(content.resource_ref)return sendResource(base,target,args.resource);
    let chunks:Uint8Array[];
    try{const negotiation=await base.talk.negotiateE2EEPublicKey({mid:target});if(negotiation.specVersion!==2||!negotiation.publicKey)throw new LineFailure('line_e2ee_required');chunks=await base.e2ee.encryptE2EEMessage(target,value,'NONE');}
    catch(error){if(error instanceof LineFailure)throw error;throw new LineFailure('line_encryption_unavailable');}
    try{
     const message=await base.talk.sendMessage({to:target,chunks,e2ee:true,contentType:'NONE',...(content.reply_to?{relatedMessageId:text(content.reply_to,128)}:{}),contentMetadata:{e2eeVersion:'2',contentType:'0',e2eeMark:'2'}});
     if(!message.id)throw new Error('ack missing');return {message_id:String(message.id)};
    }catch{throw new LineFailure('line_delivery_unknown',false)}
   })();this.sends.set(id,{fingerprint,result});return result;
  }
  if(method==='poll_events'){
   const targets=Array.isArray(args.targets)?args.targets.map(v=>text(v,128)):[];
   if(!targets.length||targets.length>100)throw new LineFailure('line_subscription_invalid');
   const revision=(value:unknown):bigint=>{
    if(typeof value==='number'&&!Number.isSafeInteger(value))throw new LineFailure('line_cursor_invalid');
    if(!['number','bigint','string'].includes(typeof value)||!/^\d{1,19}$/.test(String(value)))throw new LineFailure('line_cursor_invalid');
    const n=BigInt(String(value));if(n<0n||n>9223372036854775807n)throw new LineFailure('line_cursor_invalid');return n;
   };
   let cursor={revision:0n,globalRev:0n,individualRev:0n};
   if(args.cursor){const saved=object(JSON.parse(text(args.cursor,4096)));cursor={revision:revision(saved.revision),globalRev:revision(saved.globalRev),individualRev:revision(saved.individualRev)}}
   const response=await base.talk.sync({...cursor,limit:100,timeout:25000}),events:ServiceEvent[]=[];
   const op=response.operationResponse;
   if(response.fullSyncResponse?.nextRevision)cursor.revision=revision(response.fullSyncResponse.nextRevision);
   if(op?.globalEvents?.lastRevision)cursor.globalRev=revision(op.globalEvents.lastRevision);
   if(op?.individualEvents?.lastRevision)cursor.individualRev=revision(op.individualEvents.lastRevision);
   for(const event of op?.operations??[]){
    cursor.revision=revision(event.revision);
    const raw=event.message;if(!raw){
     if(event.param1&&targets.includes(event.param1))events.push({kind:event.type,event_key:`${event.type}:${event.param2??''}:${String(event.revision)}`,target:event.param1,code:event.type==='NOTIFIED_READ_MESSAGE'?'line_message_read':event.type==='NOTIFIED_DESTROY_MESSAGE'?'line_message_unsent':'line_chat_updated'});
     continue;
    }
    const target=raw.to===base.profile.mid?raw.from:raw.to;
    if(!target||!targets.includes(target))continue;
    const key=`${event.type}:${String(raw.id??'')}:${event.revision}`;
    try{events.push({kind:event.type,event_key:key,target,message:this.message(await base.e2ee.decryptE2EEMessage(raw),base,target)});}
    catch{events.push({kind:'decrypt_failed',event_key:key,target,code:'line_message_decryption_failed'});}
   }
   return {events,cursor:JSON.stringify(cursor,(_,v)=>typeof v==='bigint'?String(v):v),gap:!!response.fullSyncResponse};
  }
  if(method==='prepare_call'){
   const target=text(args.target,128);if(!/^u[a-f0-9]{32}$/.test(target)||target===base.profile.mid)throw new LineFailure('line_call_target_invalid');
   const sdk=await import('@evex/linejs');const {Client}=sdk;
   const client=new Client(base as unknown as ConstructorParameters<typeof Client>[0]);
   const route=await client.call.acquireRoute({to:target,callType:'AUDIO'});
   if(route.fakeCall)throw new LineFailure('line_call_route_unavailable');
   return {route,principal:base.profile.mid,target};
  }
  throw new LineFailure('line_method_unavailable');
 }
}
export function realLoader():RuntimeLoader {
 const shared=process.env.LINEJS_SHARED_DIR;if(!shared||!process.env.LINEJS_DATA_DIR||!process.env.LINEJS_KEYCHAIN_HELPER)throw new LineFailure('line_runtime_configuration_invalid');
 return async()=>{
  try{const module=await import(pathToFileURL(join(resolve(shared),'credentials.mjs')).href) as {credentials():Promise<Runtime>};return await module.credentials();}
  catch(error){
   const message=error instanceof Error?error.message:'';
   const code=(error as NodeJS.ErrnoException)?.code;
   if(code==='ENOENT'||message==='existing_login_required')throw new LineFailure('line_login_required');
   const allowed=new Set(['credential_store_busy','keychain_unavailable','credential_save_failed','invalid_encrypted_storage','credential_lock_invalid']);
   throw new LineFailure(allowed.has(message)?'line_'+message:'line_credentials_unavailable');
  }
 };
}
