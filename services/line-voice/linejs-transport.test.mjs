// Encrypted synthetic transport fixture; no account, real route or network socket.
// Adapted from upstream MIT transport tests (license: TRANSPORT_PATCH.txt).
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {PlanetTransport as OriginalTransport} from '@evex/linejs/call';
import {sdkCallURL,loadPatchedTransport,patchTransportSource} from './linejs-transport.mjs';
const cryptoModule=await import(new URL('./planet/crypto.js',sdkCallURL));
const schema=await import(new URL('./planet/schema.js',sdkCallURL));
const {makeChunkHdr}=await import(new URL('./planet/framing.js',sdkCallURL));
const {aesCtrEncrypt,buildDirectionLabel,buildPlanetCtrIv,deriveCallKeys,generateEphemeralKeypair,hmacTag}=cryptoModule;
const {CC_MSG,MC_MSG,decodePlanetMsg,packPlanetMsg,packPlanetCcMsg,packPlanetMcMsg,wrapCcMsg,wrapMcMsg,packCcConnReq,packCcInfoReq,packCcSetupRsp,packMcDataReq}=schema;
const join=(...parts)=>{const result=new Uint8Array(parts.reduce((n,p)=>n+p.length,0));let offset=0;for(const p of parts){result.set(p,offset);offset+=p.length;}return result;};
function wire(keys,plain,seq,bootstrap){
  const cipher=aesCtrEncrypt(keys.encKey,buildPlanetCtrIv(keys.ctrBase,seq),plain);
  const prefix=bootstrap?join(buildDirectionLabel(bootstrap.label),bootstrap.seed,bootstrap.pub):new Uint8Array();
  const sec=bootstrap?Uint8Array.of(0,0,0,0x28|((plain.length>>>8)&7),plain.length&255):new Uint8Array();
  const size=6+prefix.length+sec.length+cipher.length+16;
  const chunk=makeChunkHdr((((size-4)<<5)|(bootstrap?0x1d:0x0d))&0xffff);
  const tail=bootstrap?0x0602:((0x18|((plain.length>>>8)&7))<<8)|(plain.length&255);
  const header=Uint8Array.of(chunk&255,chunk>>>8,seq>>>8,seq&255,tail>>>8,tail&255);
  const authenticated=join(header,prefix,sec,cipher);
  return join(authenticated,hmacTag(keys.macKey,authenticated));
}
function message({id,session,kind='cc',tag,body,tranSeq=id}){
  const header={userId:'synthetic-peer',msgId:id,sessId:session,tranId:new Uint8Array(16).fill(id),tranSeq,locNonce:123n,rmtNonce:0n};
  const channel={cid:'synthetic-call',srcChanId:11n,dstChanId:22n};
  return packPlanetMsg(header,{kind,data:kind==='cc'?packPlanetCcMsg(channel,wrapCcMsg(tag,body)):packPlanetMcMsg(channel,wrapMcMsg(tag,body))});
}
async function exercise(Transport){
  const peer=generateEphemeralKeypair(),seed=new Uint8Array(16).fill(0x32),one=new Uint8Array(16).fill(0x51),two=new Uint8Array(16).fill(0x52);
  const connBody=packCcConnReq({netType:1,unavailToSec:120,oCapas:[],features:[]});
  const requests=[
    message({id:2,session:one,tag:CC_MSG.CONN_REQ,body:connBody,tranSeq:0}),
    message({id:3,session:two,kind:'mc',tag:MC_MSG.DATA_REQ,body:packMcDataReq({dispatchId:2,data:Uint8Array.of(0)})}),
    message({id:4,session:one,tag:CC_MSG.INFO_REQ,body:packCcInfoReq({bodyType:'profile',body:Uint8Array.of(1),targets:[],tgtUe:[]})}),
    message({id:5,session:two,kind:'mc',tag:MC_MSG.JOIN_REQ,body:Uint8Array.of(8,0)}),
    message({id:6,session:one,kind:'mc',tag:MC_MSG.CHANGE_REQ,body:Uint8Array.of(8,0)})
  ];
  const retry=message({id:7,session:two,tag:CC_MSG.CONN_REQ,body:connBody});
  const outbound=[];let keys,sequence=1,probe=0,keepalives=0,complete;
  const retried=new Promise(resolve=>{complete=resolve;});
  const transport=new Transport({localMid:'synthetic-local',timeoutMs:500,keepaliveIntervalMs:40,wireSend(packet,endpoint){
    if(endpoint.bootstrap){
      outbound.push(decodePlanetMsg(endpoint.plaintext));
      keys=deriveCallKeys({mpkey:packet.subarray(24,57),local:peer,bootstrapSeed:seed,sendLabel:7,recvLabel:7}).send;
      return wire(keys,message({id:1,session:one,tag:CC_MSG.SETUP_RSP,body:packCcSetupRsp({result:0})}),sequence++,{seed,label:7,pub:peer.publicKey});
    }
    if(endpoint.plainLen===519||endpoint.plainLen===10){const request=requests[probe++];return request?wire(keys,request,sequence++):undefined;}
    const decoded=decodePlanetMsg(endpoint.plaintext);outbound.push(decoded);
    if(decoded.hdr?.msgId===0x2244&&outbound.filter(m=>m.hdr?.msgId===0x2244).length===2)complete();
    if(decoded.hdr?.msgId===0x1101&&++keepalives===2)return wire(keys,retry,sequence++);
  }});
  try{
    await transport.connect({route:{voipAddress:'127.0.0.1',voipUdpPort:9,voipAddress6:'',toMid:'synthetic-peer',fromToken:'synthetic-token',fromZone:'JP',toZone:'JP',commParam:JSON.stringify({mpkey:Buffer.from(peer.publicKey).toString('base64')}),stid:'synthetic',stnpk:'synthetic'}});
    await transport.inviteDetailed({to:'synthetic-peer'});
    await transport.waitForAnswerDetailed({timeoutMs:500});
    let timer;try{await Promise.race([retried,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('fixture_retry_timeout')),1000);})]);}finally{clearTimeout(timer);}
  }finally{await transport.close();}
  return {outbound,requests:[...requests,retry]};
}
test('unpatched pinned SDK reproduces missing reply transaction correlation',async()=>{
  const {outbound,requests}=await exercise(OriginalTransport);
  const reply=outbound.find(m=>m.hdr?.msgId===0x2244),request=decodePlanetMsg(requests[0]);
  assert.notDeepEqual(reply.hdr.tranId,request.hdr.tranId);
});
test('patched encrypted transport correlates all five reply types and duplicate CONN requests',async()=>{
  const {PlanetTransport}=await loadPatchedTransport();
  const {outbound,requests}=await exercise(PlanetTransport),responseIds=[0x2244,0x3289,0x2247,0x3285,0x3286,0x2244];
  for(let i=0;i<requests.length;i++){
    const request=decodePlanetMsg(requests[i]);
    const reply=outbound.find(m=>m.hdr?.msgId===responseIds[i]&&Buffer.from(m.hdr.tranId).equals(Buffer.from(request.hdr.tranId)));
    assert.ok(reply,`missing correlated reply ${i}`);
    assert.equal(reply.hdr.tranSeq,request.hdr.tranSeq);assert.deepEqual(reply.hdr.sessId,request.hdr.sessId);
    assert.equal(reply.hdr.locNonce,outbound[0].hdr.locNonce);assert.equal(reply.hdr.rmtNonce,123n);
  }
  const fresh=outbound.filter(m=>!responseIds.includes(m.hdr?.msgId));
  assert.ok(fresh.some(m=>m.hdr.msgId===0x1101),'keepalive remains an independent request');
  assert.ok(fresh.some(m=>m.cc?.bodyTag===CC_MSG.INFO_REQ),'new INFO remains independent');
  assert.deepEqual(fresh.map(m=>m.hdr.tranSeq-fresh[0].hdr.tranSeq),fresh.map((_,i)=>i));
  assert.equal(new Set(fresh.map(m=>Buffer.from(m.hdr.tranId).toString('hex'))).size,fresh.length);
});
test('changed dependency source is rejected instead of silently running without the repair',async()=>{
  const source=await readFile(new URL('./planet/transport.js',sdkCallURL),'utf8');
  assert.throws(()=>patchTransportSource(source+'\n'),/baseline_mismatch/);
  const patched=patchTransportSource(source);assert.throws(()=>patchTransportSource(patched),/baseline_mismatch/);
});
