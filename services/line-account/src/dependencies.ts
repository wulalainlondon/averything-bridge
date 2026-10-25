import {createRequire} from 'node:module';
import {readFile} from 'node:fs/promises';
import {dirname,join} from 'node:path';
import {LineFailure} from './types.js';

const require=createRequire(import.meta.url);
export async function verifyDependencies(){
 if(Number(process.versions.node.split('.')[0])!==26)throw new LineFailure('line_node_version_unverified');
 const packages:{name:string;version:string;entry?:string;parents?:number}[]=[
  {name:'@evex/linejs',version:'3.4.2',parents:1},
  {name:'@jsr/evex__linejs-types',version:'3.5.0',parents:0},
  {name:'undici',version:'7.30.0',entry:'undici/package.json',parents:0},
  {name:'thrift',version:'0.23.0',entry:'thrift/package.json',parents:0},
 ];
 for(const pkg of packages){
  const entry=require.resolve(pkg.entry??pkg.name);let root=dirname(entry);for(let n=0;n<(pkg.parents??0);n++)root=dirname(root);
  const path=pkg.entry?entry:join(root,'package.json');const metadata=JSON.parse(await readFile(path,'utf8')) as {version?:unknown};
  if(metadata.version!==pkg.version)throw new LineFailure('line_sdk_version_unverified');
 }
}
