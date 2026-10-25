import {open,readFile,mkdir,rmdir,unlink} from 'node:fs/promises';
import {join} from 'node:path';

export async function acquireProfileLock(directory){
 const path=join(directory,'run.lock'),recovery=join(directory,'run.lock.recovery');
 const create=async()=>{const f=await open(path,'wx',0o600);try{await f.writeFile(JSON.stringify({pid:process.pid}));await f.sync()}finally{await f.close()}};
 try{await create()}catch(error){
  if(error.code!=='EEXIST')throw error;
  // Serialize stale-owner recovery so two recoverers cannot unlink a newly
  // created successor lock after both observed the same dead predecessor.
  try{await mkdir(recovery,{mode:0o700})}catch{throw new Error('credential_store_busy')}
  try{
   let owner;try{owner=JSON.parse(await readFile(path,'utf8'))}catch(error){if(error.code!=='ENOENT')throw new Error('credential_lock_invalid')}
   if(owner){if(!Number.isSafeInteger(owner.pid)||owner.pid<1)throw new Error('credential_lock_invalid');try{process.kill(owner.pid,0);throw new Error('credential_store_busy')}catch(error){if(error.code!=='ESRCH')throw error}await unlink(path)}
   await create();
  }finally{await rmdir(recovery)}
 }
 let released=false;return async()=>{if(released)return;released=true;const owner=await readFile(path,'utf8').then(JSON.parse).catch(()=>null);if(owner?.pid===process.pid)await unlink(path)};
}
