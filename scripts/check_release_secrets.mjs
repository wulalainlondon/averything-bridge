import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

export function containsPrivateKey(bytes) {
  const text = bytes.toString('latin1')
  return /["']private_key["']\s*:\s*["']-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/s.test(text)
    || /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----(?:\\[rn]|\s)+[A-Za-z0-9+/=]{64}/s.test(text)
}

export function containsReleaseSecret(bytes) {
  return containsPrivateKey(bytes)
    || /["']credential["']\s*:\s*["']prc_[A-Za-z0-9_-]{43}["']/.test(bytes.toString('latin1'))
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const artifacts = process.argv.slice(2)
  if (!artifacts.length) { console.error('usage: node check_release_secrets.mjs <artifact>...'); process.exitCode = 2 }
  for (const artifact of artifacts) {
    try {
      if (!fs.statSync(artifact).isFile()) throw new Error('not a regular artifact')
      if (containsReleaseSecret(fs.readFileSync(artifact))) {
        // Never print the matched bytes, key ID, account or credential JSON.
        console.error(`refusing to release artifact containing private credential material: ${artifact}`)
        process.exitCode = 1
      }
    } catch {
      console.error(`could not safely inspect release artifact: ${artifact}`)
      process.exitCode = 1
    }
  }
}
