// Offline integration fixture. No credentials, LINE API or PLANET transport.
import {normalizer,linePayload,opusFromLine} from './audio.mjs';
import {createInterface} from 'node:readline';
const a=await normalizer(),b=await normalizer();
for await(const text of createInterface({input:process.stdin})){
 const f=JSON.parse(text);
 if(f.type==='start')process.stdout.write(JSON.stringify({type:'state',state:'connected'})+'\n');
 if(f.type==='audio')for(const out of a.push(new Uint8Array(Buffer.from(f.data,'base64'))))for(const back of b.push(opusFromLine(linePayload(out))))process.stdout.write(JSON.stringify({type:'audio',data:Buffer.from(back).toString('base64'),samples:960})+'\n');
}
a.close();b.close();
