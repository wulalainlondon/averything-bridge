import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {readFile,stat} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {acquireProfileLock} from './profile-lock.mjs';
import {ProtectedStorage} from './protected-storage.mjs';
import {BaseClient} from '@evex/linejs/base';
import {Agent} from 'undici';

export async function credentials(){
  if(!process.env.LINEJS_DATA_DIR)throw new Error('credential_directory_required');
  const dir=resolve(process.env.LINEJS_DATA_DIR);
  await stat(join(dir,'session.enc')); // Never create a replacement login or key.
  const releaseLock=await acquireProfileLock(dir);
  let client,agent,key;const abort=new AbortController();let saves=Promise.resolve(),saveFailure;
  const close=async()=>{
    abort.abort();if(client)client.disabled=true;
    await saves;await agent?.destroy();key?.fill(0);
    await releaseLock();
  };
  try{
    if(process.platform==='darwin'){
      const helper=process.env.LINEJS_KEYCHAIN_HELPER;if(!helper)throw new Error('keychain_helper_required');
      const service='tw.wulala.linejs-lab.'+createHash('sha256').update(dir).digest('hex').slice(0,20);
      key=await new Promise((resolveKey,reject)=>{
        const p=spawn(helper,['get',service],{stdio:['ignore','pipe','ignore']});let data='';
        const timer=setTimeout(()=>p.kill(),10000);
        p.stdout.on('data',chunk=>{data+=chunk;if(data.length>100)p.kill();});
        p.on('error',()=>{clearTimeout(timer);reject(new Error('keychain_unavailable'));});
        p.on('close',code=>{clearTimeout(timer);const bytes=Buffer.from(data,'base64');if(code!==0||bytes.length!==32)reject(new Error('keychain_unavailable'));else resolveKey(bytes);});
      });
    }else{
      const secretDir=process.env.CREDENTIALS_DIRECTORY;
      if(!secretDir)throw new Error('systemd_credentials_required');
      const path=join(secretDir,'linejs-master-key'),info=await stat(path);
      if(info.mode&0o077)throw new Error('credential_permissions_invalid');
      key=await readFile(path);if(key.length!==32)throw new Error('credential_key_invalid');
    }
    const storage=await new ProtectedStorage(dir,key).load();
    agent=new Agent({allowH2:false,connectTimeout:15000,maxResponseSize:9*1024*1024});
    client=new BaseClient({device:'ANDROIDSECONDARY',storage,fetch:request=>fetch(request,{dispatcher:agent,signal:AbortSignal.any([request.signal,abort.signal,AbortSignal.timeout(30000)])})});
    client.on('update:authtoken',token=>{client.authToken=token;saves=saves.then(()=>storage.set('lab.authToken',token)).catch(()=>{saveFailure=true;});});
    client.e2ee.registerE2EEKeyPair=async()=>{throw new Error('e2ee_registration_disabled');};
    client.e2ee.tryRegisterE2EEGroupKey=async()=>{throw new Error('group_key_registration_disabled');};
    const refresh=client.auth.tryRefreshToken.bind(client.auth);let refreshing;
    client.auth.tryRefreshToken=()=>refreshing??=refresh().finally(()=>{refreshing=undefined;});
    const token=await storage.get('lab.authToken');if(!token)throw new Error('existing_login_required');
    await client.loginProcess.login({authToken:token});await saves;if(saveFailure)throw new Error('credential_save_failed');
    return {base:client,storage,close};
  }catch(error){await close().catch(()=>{});throw error;}
}

export async function resolvePeer(base,name){
  const wanted=name.trim().normalize('NFKC');if(!wanted||wanted.length>128)throw new Error('invalid_peer_name');
  const response=await base.relation.getUserFriendIds({request:{blockStatus:'ALL'}});
  const mids=response.userFriendMids??[];if(mids.length>2000)throw new Error('friend_limit');
  const matches=[];
  for(let i=0;i<mids.length;i+=100){
    const contacts=await base.talk.getContacts({mids:mids.slice(i,i+100)});
    for(const c of contacts)if([c.displayName,c.displayNameOverridden].some(v=>typeof v==='string'&&v.normalize('NFKC')===wanted))matches.push(c);
  }
  if(matches.length!==1||!/^u[0-9a-f]{32}$/.test(matches[0].mid)||matches[0].mid===base.profile.mid)throw new Error('peer_not_unique');
  return matches[0];
}
