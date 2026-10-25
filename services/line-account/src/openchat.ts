import {type Base,type Runtime,type RawMessage,LineFailure,object,text} from './types.js';

function joined(value:unknown){return value==='JOINED'||value===1}
export async function verifyRoom(base:Base,target:string){
 const response=object(await base.square.getSquareChat({squareChatMid:target})),room=object(response.squareChat),member=object(response.squareChatMember);
 if(room.squareChatMid!==target||!joined(member.membershipState))throw new LineFailure('line_openchat_membership_required');
 const community=object(await base.square.getSquare({squareMid:text(room.squareMid,128)}));
 if(!joined(object(community.myMembership).membershipState))throw new LineFailure('line_openchat_membership_required');
 return room;
}
export async function findRooms(runtime:Runtime,query:string,limit:number){
 const candidates=new Map<string,{id:string;name:string;kind:string}>();
 const discover=(value:unknown)=>{if(!value)return;const result=object(value);if(Array.isArray(result.events))for(const item of result.events){const e=object(item),payload=e.payload?object(e.payload):{};const created=payload.notifiedCreateSquareChatMember?object(payload.notifiedCreateSquareChatMember):{};const chat=created.chat?object(created.chat):{};if(typeof chat.squareChatMid==='string')candidates.set(chat.squareChatMid,{id:chat.squareChatMid,name:typeof chat.name==='string'?chat.name:'OpenChat',kind:'openchat'})}};
 discover(await runtime.storage.get('lab.openchat.myEventsDiscovery'));
 const selected=await runtime.storage.get('lab.openchat.selectedRoom');if(selected){const chat=object(object(selected).squareChat);if(typeof chat.squareChatMid==='string')candidates.set(chat.squareChatMid,{id:chat.squareChatMid,name:typeof chat.name==='string'?chat.name:'OpenChat',kind:'openchat'})}
 discover(await runtime.base.square.fetchMyEvents({limit:100}));
 const out=[];for(const candidate of [...candidates.values()].slice(0,20)){
  try{const room=await verifyRoom(runtime.base,candidate.id);candidate.name=typeof room.name==='string'?room.name:candidate.name;if(candidate.name.normalize('NFKC').toLocaleLowerCase().includes(query))out.push(candidate)}catch(error){if(!(error instanceof LineFailure&&error.code==='line_openchat_membership_required'))throw error}
  if(out.length>=limit)break;
 }
 return out;
}
export async function roomEvents(base:Base,target:string,cursor:string,limit:number,direction:'FORWARD'|'BACKWARD'='FORWARD'){
 await verifyRoom(base,target);
 const response=object(await base.square.fetchSquareChatEvents({squareChatMid:target,...(cursor?{syncToken:cursor}:{}),limit,direction:cursor?'FORWARD':direction}));
 const messages:{kind:string;key:string;raw:RawMessage}[]=[];
 for(const item of Array.isArray(response.events)?response.events:[]){
  const event=object(item),payload=event.payload?object(event.payload):{};
  for(const key of ['receiveMessage','sendMessage']){
   if(!payload[key])continue;const value=object(object(payload[key]).squareMessage),raw=object(value.message) as RawMessage;
   if(raw.to!==target)throw new LineFailure('line_openchat_scope_mismatch');
   messages.push({kind:String(event.type??key),key:`${String(event.type??key)}:${String(raw.id??'')}:${String(value.squareMessageRevision??event.createdTime??'')}`,raw});
  }
 }
 const next=typeof response.syncToken==='string'?response.syncToken:cursor;
 if(next.length>4096)throw new LineFailure('line_cursor_invalid');
 return {messages,cursor:next};
}
