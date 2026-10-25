import test from 'node:test';
import assert from 'node:assert/strict';
import {AccountService} from '../dist/account.js';
const peer='u'+'a'.repeat(32);
function fixture(self='u'+'b'.repeat(32)){
 const calls={opened:0,sent:0,closed:0};let sendError;
 const base={profile:{mid:self,displayName:'fixture'},relation:{getUserFriendIds:async()=>({userFriendMids:[peer]})},talk:{getContacts:async()=>[{mid:peer,displayName:'Same Name'}],getRecentMessagesV2:async()=>[{id:'m1',from:peer,to:self,text:'test',createdTime:'123'}],negotiateE2EEPublicKey:async()=>({specVersion:2,publicKey:{}}),sendMessage:async()=>{calls.sent++;if(sendError)throw sendError;return {id:'ack'};},sync:async()=>({operationResponse:{operations:[{type:'RECEIVE_MESSAGE',revision:1,message:{id:'m1',from:peer,to:self,text:'hello',createdTime:'123'}}]}})},e2ee:{encryptE2EEMessage:async()=>[new Uint8Array([1])],decryptE2EEMessage:async m=>m}};
 const service=new AccountService(async()=>{calls.opened++;return {base,storage:{get:async()=>null,set:async()=>{}},close:async()=>{calls.closed++}}});
 return {service,calls,base,failSend:()=>{sendError=new Error('raw private SDK details')}};
}
test('each account owns one resumed client and closing A leaves B available',async()=>{
 const a=fixture(),b=fixture('u'+'c'.repeat(32));
 try{await Promise.all([a.service.invoke('resume'),a.service.invoke('find_targets',{query:'Same',limit:10}),b.service.invoke('resume')]);assert.equal(a.calls.opened,1);assert.equal(b.calls.opened,1);await a.service.close();assert.equal(a.calls.closed,1);assert.equal((await b.service.invoke('resume')).principal,'u'+'c'.repeat(32));}finally{await b.service.close();}
});
test('concurrent duplicate send reuses the same effect; uncertain outcome remains consumed',async()=>{
 const f=fixture(),args={operation_id:'lo_'+'1'.repeat(32),target:peer,content:{text:'fixture'}};
 try{const results=await Promise.all([f.service.invoke('send_message',args),f.service.invoke('send_message',args)]);assert.equal(f.calls.sent,1);assert.deepEqual(results,[{message_id:'ack'},{message_id:'ack'}]);await assert.rejects(f.service.invoke('send_message',{...args,content:{text:'changed'}}),/line_intent_conflict/);
 f.failSend();const unknown={...args,operation_id:'lo_'+'2'.repeat(32)};await assert.rejects(f.service.invoke('send_message',unknown),e=>e.code==='line_delivery_unknown'&&!e.beforeEffect);await assert.rejects(f.service.invoke('send_message',unknown),/line_delivery_unknown/);assert.equal(f.calls.sent,2);
 }finally{await f.service.close();}
});
test('history retrieval does not mark read and polling scopes target content with versioned keys',async()=>{
 const f=fixture();try{const result=await f.service.invoke('get_messages',{target:peer,kind:'user',limit:3});assert.equal(result[0].direction,'inbound');const batch=await f.service.invoke('poll_events',{targets:[peer],cursor:''});assert.equal(batch.events.length,1);assert.equal(batch.events[0].event_key,'RECEIVE_MESSAGE:m1:1');assert.equal(JSON.parse(batch.cursor).revision,'1');const other=await f.service.invoke('poll_events',{targets:['u'+'d'.repeat(32)],cursor:''});assert.equal(other.events.length,0);await assert.rejects(f.service.invoke('get_messages',{target:peer,kind:'user',limit:3,cursor:'older'}),/pagination_unverified/);}finally{await f.service.close();}
});
test('late credential resolution is disposed after account closure',async()=>{
 let resolve,closed=0;const runtime={base:{profile:{mid:peer}},close:async()=>{closed++}};const service=new AccountService(()=>new Promise(r=>{resolve=r}));const waiting=service.invoke('resume');const closing=service.close();resolve(runtime);await assert.rejects(waiting,/line_account_closed/);await closing;assert.equal(closed,1);
});

test('checkpoints preserve 64 bit revisions and reject unsafe numeric truncation',async()=>{
 const f=fixture();try{
  f.base.talk.sync=async()=>({fullSyncResponse:{nextRevision:9007199254740993n},operationResponse:{globalEvents:{lastRevision:9007199254740994n},individualEvents:{lastRevision:9007199254740995n}}});
  const batch=await f.service.invoke('poll_events',{targets:[peer],cursor:''});assert.equal(batch.gap,true);const saved=JSON.parse(batch.cursor);assert.equal(saved.revision,'9007199254740993');assert.equal(saved.globalRev,'9007199254740994');
  f.base.talk.sync=async args=>{assert.equal(args.revision,9007199254740993n);return {}};await f.service.invoke('poll_events',{targets:[peer],cursor:batch.cursor});
  await assert.rejects(f.service.invoke('poll_events',{targets:[peer],cursor:JSON.stringify({revision:Number.MAX_SAFE_INTEGER+1,globalRev:0,individualRev:0})}),/line_cursor_invalid/);
 }finally{await f.service.close()}
});

test('non-message receipts retain operation revision and type instead of collapsing message ids',async()=>{const f=fixture();try{f.base.talk.sync=async()=>({operationResponse:{operations:[{type:'NOTIFIED_READ_MESSAGE',revision:2,param1:peer,param2:'m1'},{type:'NOTIFIED_DESTROY_MESSAGE',revision:3,param1:peer,param2:'m1'}]}});const b=await f.service.invoke('poll_events',{targets:[peer],cursor:''});assert.equal(b.events.length,2);assert.notEqual(b.events[0].event_key,b.events[1].event_key);assert.equal(b.events[0].code,'line_message_read');assert.equal(b.events[1].code,'line_message_unsent')}finally{await f.service.close()}});
