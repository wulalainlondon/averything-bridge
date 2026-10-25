import {copyFile,rename,open,stat,chmod} from 'node:fs/promises';
import {join} from 'node:path';
import {LineFailure} from './types.js';

// Credential bytes stay encrypted. A wrong-principal scan cannot replace the
// original login, even after the upstream QR exchange itself has completed.
export async function commitStagedLogin(directory:string,staging:string,actual:string,expected:string){
 if(!actual||actual!==expected)throw new LineFailure('line_principal_mismatch');
 const source=join(staging,'session.enc'),target=join(directory,'session.enc');
 const info=await stat(source);if(!info.isFile()||info.size<32)throw new LineFailure('line_pairing_storage_invalid');
 const existed=await stat(target).then(()=>true,error=>{if(error.code==='ENOENT')return false;throw error});
 if(existed){const backup=join(directory,'session.before-reauth.enc.tmp');await copyFile(target,backup);await chmod(backup,0o600);await rename(backup,join(directory,'session.before-reauth.enc'))}
 await rename(source,target);await chmod(target,0o600);
 const folder=await open(directory,'r');try{await folder.sync()}finally{await folder.close()}
}
