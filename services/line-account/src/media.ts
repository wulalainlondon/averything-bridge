import {createHash} from 'node:crypto';
import {type Base,type RawMessage,LineFailure,object,text} from './types.js';

// A call-local OBS client proxy observes the upload → chat-send boundary
// without monkey-patching the shared account client or competing sends.
export async function sendResource(base:Base,target:string,input:unknown):Promise<{message_id:string}>{
 const resource=object(input),encoded=text(resource.data,12*1024*1024),name=text(resource.name,240),mime=text(resource.mime,128);
 const bytes=Buffer.from(encoded,'base64');
 if(!bytes.length||bytes.length>8*1024*1024||createHash('sha256').update(bytes).digest('hex')!==resource.sha256)throw new LineFailure('line_resource_integrity');
 let chatStarted=false;
 const bind=(target:object,key:string|symbol)=>{const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value};
 const talk=new Proxy(base.talk,{get(t,p){if(p==='sendMessage')return async(args:Record<string,unknown>)=>{chatStarted=true;return t.sendMessage({...args,e2ee:true})};return bind(t,p)}});
 const account=new Proxy(base,{get(t,p){return p==='talk'?talk:bind(t,p)}});
 const Constructor=base.obs.constructor as new(client:Base)=>{uploadMediaByE2EE(input:unknown):Promise<RawMessage>};
 const type=mime==='image/gif'?'gif':mime.startsWith('image/')&&!mime.includes('svg')?'image':'file';
 try{const result=await new Constructor(account).uploadMediaByE2EE({to:target,oType:type,data:new Blob([new Uint8Array(bytes)],{type:mime}),filename:name});if(!result.id)throw new Error('ack missing');return {message_id:String(result.id)}}
 catch{throw new LineFailure(chatStarted?'line_media_chat_delivery_unknown':'line_media_upload_failed',!chatStarted)}
 finally{bytes.fill(0)}
}
