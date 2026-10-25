#!/usr/bin/env node
// Creates a new dependency-pinned runtime artifact. Never copies account data.
import {spawnSync} from 'node:child_process';
import {readFile,writeFile,mkdir,readdir,copyFile,stat,realpath,rm} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {dirname,resolve,join,relative} from 'node:path';
import {fileURLToPath} from 'node:url';

const source=resolve(dirname(fileURLToPath(import.meta.url)),'..'),output=process.argv[2]&&resolve(process.argv[2]);
if(!output||Number(process.versions.node.split('.')[0])!==26)throw new Error('usage: node26 stage_line_runtime.mjs <new-output-directory>');
try{await stat(output);throw new Error('output already exists')}catch(error){if(error.code!=='ENOENT')throw error}
const run=(args,cwd)=>{const result=spawnSync('npm',args,{cwd,stdio:['ignore','inherit','inherit']});if(result.status!==0)throw new Error('dependency build failed')};
async function copyTree(from,to){await mkdir(to,{recursive:true});for(const entry of await readdir(from,{withFileTypes:true})){if(entry.isSymbolicLink())throw new Error('unexpected source symlink');if(entry.isDirectory())await copyTree(join(from,entry.name),join(to,entry.name));else if(entry.isFile())await copyFile(join(from,entry.name),join(to,entry.name))}}
await mkdir(output,{recursive:true,mode:0o700});
try{
 const account=join(source,'services/line-account'),voice=join(source,'services/line-voice');run(['ci','--ignore-scripts'],account);run(['run','build'],account);
 for(const [from,to] of [[account,join(output,'account')],[voice,join(output,'voice')]]){
  await mkdir(to);for(const name of ['package.json','package-lock.json'])await copyFile(join(from,name),join(to,name));run(['ci','--omit=dev','--ignore-scripts'],to);
 }
 await copyTree(join(account,'dist'),join(output,'account/dist'));
 for(const name of ['route-worker.mjs','session.mjs','audio.mjs','credentials.mjs','protected-storage.mjs','profile-lock.mjs','linejs-transport.mjs','srtp-rollover.mjs','TRANSPORT_PATCH.txt'])await copyFile(join(voice,name),join(output,'voice',name));
 const files={};
 async function hashTree(path){for(const entry of await readdir(path,{withFileTypes:true})){if(entry.isSymbolicLink())continue;const item=join(path,entry.name);if(entry.isDirectory())await hashTree(item);else if(entry.isFile())files[relative(output,item).split('\\').join('/')]=createHash('sha256').update(await readFile(item)).digest('hex')}}
 await hashTree(output);const nodePath=await realpath(process.execPath),nodeHash=createHash('sha256').update(await readFile(nodePath)).digest('hex');
 await writeFile(join(output,'manifest.json'),JSON.stringify({schema_version:1,node:{version:process.version,sha256:nodeHash},files},null,2)+'\n',{mode:0o600});
 process.stdout.write(`LINE runtime staged: ${Object.keys(files).length} hashed files; no account profiles or keys included\n`);
}catch(error){await rm(output,{recursive:true,force:true});throw error}
