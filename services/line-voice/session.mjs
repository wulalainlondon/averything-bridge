import {createHash} from 'node:crypto';
export class VoiceSession {
  constructor(deps){this.deps=deps;this.stopped=false;this.state='idle';this.queue=[];this.sending=false;}
  check(){if(this.stopped)throw new Error('call_cancelled');}
  emit(state){this.state=state;this.deps.emit({type:'state',state});}
  async stage(name,operation){
    const started=performance.now();
    const report=(status,errorCode)=>this.deps.emit({type:'diagnostic',code:'line_call_stage',stage:name,status,elapsedMs:Math.round(performance.now()-this.startedAt),durationMs:Math.round(performance.now()-started),...(errorCode?{errorCode}:{})});
    report('started');
    try{const result=await operation();this.check();report('completed');return result;}
    catch(error){
      const message=error?.message??'';
      const code=message==='PLANET reply timeout'?`line_${name}_timeout`:/^[a-z_]{1,64}$/.test(message)?message:`line_${name}_failed`;
      report('failed',code);throw new Error(code);
    }
  }
  async start({callId,peerName}){
    if(this.state!=='idle'||!/^line_[a-f0-9]{64}$/.test(callId))throw new Error('invalid_call_intent');
    this.startedAt=performance.now();
    this.emit('starting');
    await this.stage('credentials',async()=>{this.runtime=await this.deps.credentials();});
    const peer=await this.stage('peer',()=>this.deps.resolvePeer(this.runtime.base,peerName));
    const slot='bridge.lineVoice.'+callId;this.slot=slot;
    // Every attempt, including a previous finished call, consumes its durable ID.
    if(await this.runtime.storage.get(slot))throw new Error('call_intent_already_used');
    const fingerprint=createHash('sha256').update(JSON.stringify([this.runtime.base.profile.mid,peer.mid,peerName])).digest('hex');
    await this.runtime.storage.set(slot,{state:'preparing',fingerprint,at:new Date().toISOString()});this.check();
    const route=await this.stage('route',async()=>{const value=await this.deps.acquireRoute(this.runtime.base,peer.mid);if(value.fakeCall)throw new Error('call_route_unavailable');return value;});
    this.transport=this.deps.transport(event=>{
      if(event.type==='rel_req'&&!this.stopped){
        this.deps.emit({type:'diagnostic',code:'line_remote_release',relCode:Number.isSafeInteger(event.relCode)?event.relCode:null,commMediaFlags:Number.isSafeInteger(event.commMediaFlags)?event.commMediaFlags:null});
        void this.stop('remote_release');
      }
    });
    await this.stage('transport',()=>this.transport.connect({route}));
    await this.runtime.storage.set(slot,{state:'dispatching',fingerprint,at:new Date().toISOString()});this.check();
    await this.stage('invite',()=>this.transport.inviteDetailed({to:peer.mid}));
    // SETUP acknowledgement proves accepted signaling, not audible phone ringing.
    this.emit('ringing');
    const answer=await this.stage('answer',()=>this.transport.waitForAnswerDetailed({autoConnRsp:true,timeoutMs:45000}));
    if(!answer.mediaReady)throw new Error('line_media_unavailable');
    await this.stage('media',async()=>{this.toLine=await this.deps.normalizer();this.fromLine=await this.deps.normalizer();});
    await this.runtime.storage.set(slot,{state:'connected',fingerprint,at:new Date().toISOString()});this.check();
    this.emit('connected');
    this.receiveTask=this.receive();
  }
  audio(bytes){
    if(this.state!=='connected'||this.stopped)return;
    try{
      for(const packet of this.toLine.push(bytes)){if(this.queue.length>=8)this.queue.shift();this.queue.push(packet);}
    }catch{this.deps.emit({type:'error',code:'audio_decode_failed'});void this.stop('audio_decode_failed');return;}
    if(!this.sending)void this.drain();
  }
  async drain(){
    this.sending=true;
    try{while(this.queue.length&&!this.stopped){
      const delay=Math.max(0,(this.nextSend??0)-performance.now());
      if(delay)await new Promise(resolve=>setTimeout(resolve,delay));
      this.check();const packet=this.queue.shift();if(!packet)break;
      await this.transport.send(this.deps.linePayload(packet),{timestampStep:960});this.sentFrames=(this.sentFrames??0)+1;
      this.nextSend=performance.now()+20;
    }}catch{if(!this.stopped)this.deps.emit({type:'error',code:'audio_send_failed'});await this.stop('audio_send_failed');}
    finally{this.sending=false;}
  }
  async receive(){
    try{for await(const bytes of this.transport.receive()){
      if(this.stopped)break;
      // A single unsupported or malformed packet must not hang up a valid call.
      // Keep diagnostics structural: never include decrypted media or SDK errors.
      let opus,packets;
      try{opus=this.deps.opusFromLine(bytes);}catch{this.droppedAudio('wrapper',bytes);continue;}
      try{packets=this.fromLine.push(opus);}catch{this.droppedAudio('decode',bytes);continue;}
      for(const packet of packets){this.receivedFrames=(this.receivedFrames??0)+1;this.deps.emit({type:'audio',data:Buffer.from(packet).toString('base64'),samples:960});}
    }}catch{if(!this.stopped)this.deps.emit({type:'error',code:'audio_receive_failed'});}
    finally{await this.stop('peer_media_ended');}
  }
  droppedAudio(stage,bytes){
    this.audioDrops=(this.audioDrops??0)+1;
    if(this.audioDrops<=3||this.audioDrops%1000===0)this.deps.emit({type:'diagnostic',code:'line_audio_packet_dropped',stage,droppedPackets:this.audioDrops,payloadLength:bytes.length,nativePrefix:bytes.length?bytes[0]:null});
  }
  async stop(reason='hangup'){
    if(this.stopTask)return this.stopTask;
    this.stopped=true;this.queue=[];
    this.stopTask=(async()=>{
      await this.transport?.close().catch(()=>{});
      // A failed start can still resolve credentials later; worker's finally closes it.
      if(this.runtime&&this.slot){const old=await this.runtime.storage.get(this.slot);await this.runtime.storage.set(this.slot,{...old,state:'ended',reason,at:new Date().toISOString()});}
      this.emit('closed');
    })();return this.stopTask;
  }
  async dispose(){
    await this.stop();
    // Close again after start() has settled: an asynchronous connect can
    // finish after a cancellation issued its first close.
    await this.transport?.close().catch(()=>{});
    this.toLine?.close();this.fromLine?.close();await this.runtime?.close();
  }
}
