import assert from 'node:assert/strict'
import test from 'node:test'
import { containsPrivateKey, containsReleaseSecret } from './check_release_secrets.mjs'

test('rejects service-account private key JSON, including multiline fields', () => {
  const header = '-----BEGIN PRIVATE KEY-----'
  for (const separator of ['', ' ', '\n']) {
    assert.equal(containsPrivateKey(Buffer.from(`{"private_key":${separator}"${header}\\nFAKE"}`)), true)
  }
})
test('rejects PEM material while allowing isolated cryptographic format labels', () => {
  for (const type of ['', 'RSA ', 'EC ', 'OPENSSH ']) {
    const header = `-----BEGIN ${type}PRIVATE KEY-----`
    assert.equal(containsPrivateKey(Buffer.from(header)), false)
    assert.equal(containsPrivateKey(Buffer.from(`${header}\n${'A'.repeat(80)}`)), true)
    assert.equal(containsPrivateKey(Buffer.from(`${header}\\n${'A'.repeat(80)}`)), true)
  }
  assert.equal(containsPrivateKey(Buffer.from('json:"private_key"\0public runtime path\0fcm_service_account.json')), false)
})

test('rejects private broker identity JSON but allows runtime generation labels', () => {
  const fixture = 'prc_' + 'A'.repeat(43)
  assert.equal(containsReleaseSecret(Buffer.from(JSON.stringify({credential: fixture}))), true)
  assert.equal(containsReleaseSecret(Buffer.from(`{"credential": "${fixture}"}`)), true)
  assert.equal(containsReleaseSecret(Buffer.from('push_relay_client.json\0prc_\0credential')), false)
})
