import {opusCodecFactory} from '@evex/linejs/call';

// Normalize each direction independently to 48 kHz mono, 20 ms packets.
// libopus performs the input channel conversion; packets are never treated as PCM.
export async function normalizer(){
  const factory=await opusCodecFactory();
  const decoder=factory.newDecoder({sampleRate:48000,channels:1});
  const encoder=factory.newEncoder({sampleRate:48000,channels:1,frameDurationMs:20,bitrate:32000,signal:'voice'});
  let pending=new Int16Array(0);
  return {
    push(packet){
      if(!(packet instanceof Uint8Array)||!packet.length||packet.length>4000)throw new Error('invalid_opus_packet');
      const decoded=decoder.decode(packet);
      if(decoded.samples.length>5760)throw new Error('oversized_audio_frame');
      const joined=new Int16Array(pending.length+decoded.samples.length);joined.set(pending);joined.set(decoded.samples,pending.length);
      const output=[];let offset=0;
      for(;offset+960<=joined.length;offset+=960){const encoded=encoder.encode({samples:joined.subarray(offset,offset+960),sampleRate:48000,channels:1});if(encoded?.length)output.push(encoded);}
      pending=joined.slice(offset);return output;
    },close(){decoder.close?.();encoder.close?.();pending=new Int16Array(0);}
  };
}
export function linePayload(opus){const out=new Uint8Array(opus.length+1);out.set(opus,1);return out;}
export function opusFromLine(payload){
  // Native payloads can advance from prefix 00 to 10. Prefix validation is
  // separate from Opus validation; an accepted wrapper never proves decodability.
  if(payload.length<2||(payload[0]!==0&&payload[0]!==0x10))throw new Error('unsupported_line_audio_wrapper');
  return payload.subarray(1);
}
