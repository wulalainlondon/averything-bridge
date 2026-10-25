import test from 'node:test';
import assert from 'node:assert/strict';
import {liveTestOptions} from './live-test-options.mjs';
const required=['--peer','test','--id','stable-test','--confirm-one-call'];
test('short test stays bounded and twenty-minute hold is explicit',()=>{
  assert.equal(liveTestOptions(required).holdSeconds,10);
  assert.equal(liveTestOptions([...required,'--hold-seconds','1200']).holdSeconds,1200);
  for(const value of ['0','9','1201','-1','1e3','Infinity','10.5',''])assert.throws(()=>liveTestOptions([...required,'--hold-seconds='+value]),/invalid_hold_seconds/);
  assert.throws(()=>liveTestOptions(['--peer','test','--id','stable-test']),/confirm_one_call/);
});
