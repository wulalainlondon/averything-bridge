import {verifyDependencies} from './dependencies.js';
import {AccountService,realLoader} from './account.js';
import {LineFailure,object,errorCode} from './types.js';
console.log=()=>{};console.error=()=>{};
const account=process.env.LINE_ACCOUNT_ID,epoch=process.env.LINE_RUNTIME_EPOCH;
if(!account||!/^ln_[a-f0-9]{32}$/.test(account)||!epoch){process.exitCode=64;throw new LineFailure('line_runtime_identity_invalid')}
await verifyDependencies();
const service=new AccountService(realLoader());let buffer='',closed=false;
function emit(value:unknown){if(!closed){const encoded=JSON.stringify(value,(_,v)=>typeof v==='bigint'?String(v):v);if(Buffer.byteLength(encoded)>12*1024*1024){void finish();return;}process.stdout.write(encoded+'\n');}}
async function finish(){if(closed)return;closed=true;await service.close().catch(()=>{});process.stdin.pause();setTimeout(()=>process.exit(),50).unref();}
async function command(value:unknown){
 const request=object(value);
 const id=request.id;
 if(typeof id!=='string'||id.length>128||request.account_ref!==account||request.epoch!==epoch){emit({id,account_ref:account,epoch,error:{code:'line_runtime_identity_mismatch',before_effect:true}});return;}
 try{if(typeof request.method!=='string')throw new LineFailure('line_request_invalid');const result=await service.invoke(request.method,request.params);emit({id,account_ref:account,epoch,result});if(request.method==='close')void finish();}
 catch(error){emit({id,account_ref:account,epoch,error:{code:errorCode(error),before_effect:error instanceof LineFailure&&error.beforeEffect}});}
}
process.stdin.setEncoding('utf8');process.stdin.on('data',(chunk:string)=>{
 buffer+=chunk;if(buffer.length>12*1024*1024){void finish();return};let end:number;
 while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);try{void command(JSON.parse(line)).catch(()=>finish());}catch{void finish();}}
});
process.stdin.on('end',()=>void finish());process.stdin.on('error',()=>void finish());process.stdout.on('error',()=>void finish());
process.on('SIGTERM',()=>void finish());process.on('SIGINT',()=>void finish());
process.on('uncaughtException',()=>void finish());process.on('unhandledRejection',()=>void finish());
emit({type:'ready',protocol_version:1,account_ref:account,epoch});
