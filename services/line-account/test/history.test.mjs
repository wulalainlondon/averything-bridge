import test from 'node:test';
import assert from 'node:assert/strict';
import {messagePage} from '../dist/history.js';
const self='u'+'1'.repeat(32),peer='u'+'2'.repeat(32);
const row=(id,at)=>({id,from:peer,to:self,deliveredTime:at,createdTime:at,text:'hello'});
const convert=m=>({message_ref:m.id,target_ref:peer,text:m.text,timestamp:Number(m.createdTime)});
test('64-bit boundaries survive paging and duplicate boundary is excluded',async()=>{
 const newest=row('9007199254740995',200n),older=row('9007199254740994',100n);
 const base={profile:{mid:self},talk:{getRecentMessagesV2:async()=>[newest],getPreviousMessagesV2WithRequest:async q=>{assert.equal(q.request.endMessageId.messageId,9007199254740995n);assert.equal(q.request.withReadCount,false);return[newest,older]}},e2ee:{decryptE2EEMessage:async m=>m}};
 const first=await messagePage(base,peer,1,'',convert);assert.equal(JSON.parse(first.next_cursor).messageId,newest.id);
 const second=await messagePage(base,peer,1,first.next_cursor,convert);assert.equal(second.messages[0].message_ref,older.id);assert.equal(second.complete_history,false);
 base.talk.getPreviousMessagesV2WithRequest=async()=>[older];const end=await messagePage(base,peer,1,second.next_cursor,convert);assert.equal(end.next_cursor,'');assert.equal(end.coverage,'upstream_exhausted');
 await assert.rejects(messagePage(base,self,1,first.next_cursor,convert),/line_history_cursor_invalid/);
});
test('missing cursor metadata, decrypt errors and cross-room results are explicit',async()=>{
 const base={profile:{mid:self},talk:{getRecentMessagesV2:async()=>[{...row('1',1),deliveredTime:undefined}]},e2ee:{decryptE2EEMessage:async()=>{throw Error('private error')}}};
 const page=await messagePage(base,peer,2,'',convert);assert.equal(page.coverage,'pagination_unavailable');assert.equal(page.decryption_failures,1);assert.equal(page.messages.length,0);
 base.talk.getRecentMessagesV2=async()=>[{...row('1',1),from:'someone-else'}];await assert.rejects(messagePage(base,peer,2,'',convert),/line_history_target_mismatch/);
});
