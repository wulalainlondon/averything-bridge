// Receiver index/ROC and replay state for the exact pinned upstream SRTP source.
// RFC3711 sections3.3.1/3.3.2. Original crypto/KDF operations remain unchanged.
import {createHash} from 'node:crypto';
export const baselineSrtpSHA256='42b2221705474566fdade2f1b2470ae51b48cf23c3dc4d1351d98f1b7ae6b47e';
const hash=value=>createHash('sha256').update(value).digest('hex');
function once(source,before,after){const i=source.indexOf(before);if(i<0||source.indexOf(before,i+before.length)>=0)throw Error('line_srtp_patch_shape_invalid');return source.slice(0,i)+after+source.slice(i+before.length)}
export function patchSrtpSource(source){
 if(hash(source)!==baselineSrtpSHA256)throw Error('line_srtp_patch_baseline_mismatch');
 source=once(source,'export async function srtpDecrypt(ctx, srtpPacket) {',`const lineReceiverStates = new WeakMap();
const lineDecryptQueues = new WeakMap();
export async function srtpDecrypt(ctx, packet) {
  let queue = lineDecryptQueues.get(ctx);
  if (!queue) { queue = { tail: Promise.resolve(), pending: 0 }; lineDecryptQueues.set(ctx, queue); }
  if (queue.pending >= 64) throw new Error("SRTP decrypt queue limit");
  queue.pending++;
  const operation = queue.tail.catch(() => {}).then(() => lineDecryptVerified(ctx, packet));
  queue.tail = operation;
  try { return await operation; }
  finally { if (--queue.pending === 0 && lineDecryptQueues.get(ctx) === queue) lineDecryptQueues.delete(ctx); }
}
async function lineDecryptVerified(ctx, srtpPacket) {`);
 source=once(source,'  const roc = ctx.rocs.get(ssrc) ?? 0;',`  const streams = lineReceiverStates.get(ctx);
  const state = streams?.get(ssrc);
  const currentRoc = (state?.roc ?? ctx.rocs.get(ssrc) ?? 0) >>> 0;
  let roc = currentRoc;
  if (state) {
    if (state.seq < 32768 && seq - state.seq > 32768) roc--;
    else if (state.seq >= 32768 && state.seq - 32768 > seq) roc++;
  }
  if (roc < 0 || roc > 0xffffffff) throw new Error("SRTP index out of range");
  const index = BigInt(roc) * 65536n + BigInt(seq);`);
 source=once(source,'  const headerLen = rtpHeaderLength(inner);',`  // Authentication above must succeed before allocating/updating receive state.
  if (!state && (streams?.size ?? 0) >= 128) throw new Error("SRTP receiver stream limit");
  if (state && index <= state.highest) {
    const behind = state.highest - index;
    if (behind >= 64n || (state.bitmap & (1n << behind)) !== 0n) throw new Error("SRTP replay rejected");
  }
  const headerLen = rtpHeaderLength(inner);`);
 source=once(source,'  out.set(dec, headerLen);\n  return out;',`  out.set(dec, headerLen);
  const receiveStreams = streams ?? new Map();
  if (!streams) lineReceiverStates.set(ctx, receiveStreams);
  if (!state) receiveStreams.set(ssrc, { roc, seq, highest: index, bitmap: 1n });
  else if (index > state.highest) {
    const ahead = index - state.highest;
    state.bitmap = ahead >= 64n ? 1n : ((state.bitmap << ahead) | 1n) & ((1n << 64n) - 1n);
    state.highest = index; state.seq = seq; state.roc = roc;
  } else state.bitmap |= 1n << (state.highest - index);
  ctx.rocs.set(ssrc, receiveStreams.get(ssrc).roc);
  return out;`);
 return source;
}
