import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {sendResource} from '../dist/media.js';
const bytes=Buffer.from('private fixture file'),resource={data:bytes.toString('base64'),sha256:createHash('sha256').update(bytes).digest('hex'),name:'fixture.txt',mime:'text/plain'};
function fixture(fail){
 const calls={chat:0};const base={talk:{sendMessage:async args=>{calls.chat++;assert.equal(args.e2ee,true);if(fail==='chat')throw new Error('private transport details');return {id:'ack'}}}};
 class Obs{constructor(client){this.client=client}async uploadMediaByE2EE(input){assert.equal(input.oType,'file');assert.equal(await input.data.text(),'private fixture file');if(fail==='upload')throw new Error('private upload details');return this.client.talk.sendMessage({to:input.to})}}
 base.obs=new Obs(base);return {base,calls};
}
test('upload and uncertain chat delivery remain distinct without patching the shared client',async()=>{
 const f=fixture();const original=f.base.talk.sendMessage;assert.deepEqual(await sendResource(f.base,'u_fixture',resource),{message_id:'ack'});assert.equal(f.calls.chat,1);assert.equal(f.base.talk.sendMessage,original);
 const upload=fixture('upload');await assert.rejects(sendResource(upload.base,'u_fixture',resource),e=>e.code==='line_media_upload_failed'&&e.beforeEffect);assert.equal(upload.calls.chat,0);
 const chat=fixture('chat');await assert.rejects(sendResource(chat.base,'u_fixture',resource),e=>e.code==='line_media_chat_delivery_unknown'&&!e.beforeEffect);assert.equal(chat.calls.chat,1);
});
test('changed bytes fail integrity before upload or chat effect',async()=>{const f=fixture();await assert.rejects(sendResource(f.base,'u_fixture',{...resource,data:Buffer.from('other').toString('base64')}),/line_resource_integrity/);assert.equal(f.calls.chat,0)});
