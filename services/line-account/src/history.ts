import {LineFailure,object,text,type Base,type RawMessage,type WireMessage} from './types.js';

const int64=(v:unknown):string=>{
 if(typeof v==='number'&&!Number.isSafeInteger(v))throw new LineFailure('line_history_cursor_invalid');
 if(!['number','bigint','string'].includes(typeof v)||!/^\d{1,19}$/.test(String(v))||BigInt(String(v))>9223372036854775807n)throw new LineFailure('line_history_cursor_invalid');
 return String(v);
};
export async function messagePage(base:Base,target:string,limit:number,cursor:string,convert:(raw:RawMessage)=>WireMessage){
 let boundary:{messageId:string;deliveredTime:string}|undefined;
 if(cursor){
  try{const c=object(JSON.parse(cursor));if(c.target!==target)throw new Error();boundary={messageId:int64(c.messageId),deliveredTime:int64(c.deliveredTime)};}
  catch{throw new LineFailure('line_history_cursor_invalid')}
 }
 const rows=boundary
  ?await base.talk.getPreviousMessagesV2WithRequest({request:{messageBoxId:target,endMessageId:{messageId:BigInt(boundary.messageId),deliveredTime:BigInt(boundary.deliveredTime)},messagesCount:limit+1,withReadCount:false,receivedOnly:false}})
  :await base.talk.getRecentMessagesV2({messageBoxId:target,messagesCount:limit});
 if(rows.length>limit+1)throw new LineFailure('line_history_result_invalid');
 // LINE may include the boundary message again. Keep exact int64 comparisons.
 const seen=new Set<string>();const selected:RawMessage[]=[];
 for(const m of rows){
  const id=String(m.id??'');
  if(!id||seen.has(id)||boundary&&id===boundary.messageId)continue;
  if(!((m.from===target&&m.to===base.profile.mid)||(m.from===base.profile.mid&&m.to===target)))throw new LineFailure('line_history_target_mismatch');
  seen.add(id);selected.push(m);
 }
 // The upstream API returns newest first; reject a backwards boundary instead of looping.
 const usable=selected.slice(0,limit);const last=usable.at(-1);let next='';let coverage='upstream_exhausted';
 if(last){
  try{
   const id=int64(last.id),delivered=int64(last.deliveredTime);
   if(boundary&&(BigInt(delivered)>BigInt(boundary.deliveredTime)||(delivered===boundary.deliveredTime&&BigInt(id)>=BigInt(boundary.messageId))))throw new LineFailure('line_history_cursor_stalled');
   next=JSON.stringify({target,messageId:id,deliveredTime:delivered});coverage='upstream_page';
  }catch(e){if(e instanceof LineFailure&&e.code==='line_history_cursor_stalled')throw e;coverage='pagination_unavailable';}
 }
 const messages:WireMessage[]=[];let failures=0;
 for(const raw of usable){try{messages.push(convert(await base.e2ee.decryptE2EEMessage(raw)))}catch{failures++}}
 return {messages,next_cursor:next,scanned:usable.length,decryption_failures:failures,coverage,complete_history:false};
}
