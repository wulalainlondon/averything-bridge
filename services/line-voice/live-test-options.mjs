import {parseArgs} from 'node:util';

export function liveTestOptions(args){
  const {values}=parseArgs({args,options:{peer:{type:'string'},id:{type:'string'},'hold-seconds':{type:'string',default:'10'},'confirm-one-call':{type:'boolean'}}});
  if(!values.peer?.trim()||!values.id?.trim()||!values['confirm-one-call'])throw new Error('requires_peer_stable_id_and_confirm_one_call');
  const raw=values['hold-seconds'];
  if(!/^[0-9]+$/.test(raw))throw new Error('invalid_hold_seconds');
  const holdSeconds=Number(raw);
  if(!Number.isSafeInteger(holdSeconds)||holdSeconds<10||holdSeconds>1200)throw new Error('invalid_hold_seconds');
  return {peer:values.peer,id:values.id,holdSeconds,toneSeconds:5};
}
