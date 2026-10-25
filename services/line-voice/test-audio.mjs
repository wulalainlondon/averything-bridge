// Synthetic media only: audible test tone, then valid Opus silence for the hold.
export function testAudioSource(encoder,toneSeconds=5){
  let frame=0;
  return {next(){
    const current=frame++,tone=current<toneSeconds*50;
    const samples=tone?Int16Array.from({length:960},(_,i)=>Math.round(3000*Math.sin(2*Math.PI*440*(i+current*960)/48000))):new Int16Array(960);
    return {tone,packet:encoder.encode({sampleRate:48000,channels:1,samples})};
  }};
}
