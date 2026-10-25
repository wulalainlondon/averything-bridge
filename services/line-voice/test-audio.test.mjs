import test from 'node:test';
import assert from 'node:assert/strict';
import {opusCodecFactory} from '@evex/linejs/call';
import {testAudioSource} from './test-audio.mjs';
test('long-call source continues valid silent media after the five-second tone',async()=>{
  const codec=await opusCodecFactory(),encoder=codec.newEncoder({sampleRate:48000,channels:1,frameDurationMs:20,signal:'voice'}),decoder=codec.newDecoder({sampleRate:48000,channels:1});
  const source=testAudioSource(encoder);let toneFrames=0,silentFrames=0,tonePeak=0,silentTailPeak=0;
  try{
    for(let i=0;i<750;i++){
      const {tone,packet}=source.next();assert.ok(packet?.length);
      const pcm=decoder.decode(packet).samples;assert.equal(pcm.length,960);
      if(tone){toneFrames++;for(const sample of pcm)tonePeak=Math.max(tonePeak,Math.abs(sample));}
      else{silentFrames++;if(i>=650)for(const sample of pcm)silentTailPeak=Math.max(silentTailPeak,Math.abs(sample));}
    }
    assert.equal(toneFrames,250);assert.equal(silentFrames,500);assert.ok(tonePeak>1000);assert.ok(silentTailPeak<100);
  }finally{encoder.close();decoder.close();}
});
