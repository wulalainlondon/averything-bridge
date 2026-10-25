import {createCipheriv, createDecipheriv, randomBytes, randomUUID} from 'node:crypto';
import {mkdir, readFile, open, rename, chmod} from 'node:fs/promises';
import {join} from 'node:path';
import {BaseStorage} from '@evex/linejs/storage';

const serialize = value => JSON.stringify(value, (_, v) => typeof v === 'bigint' ? {$bigint:String(v)} : v);
const deserialize = value => JSON.parse(value, (_, v) => v && typeof v === 'object' && Object.keys(v).length === 1 && typeof v.$bigint === 'string' ? BigInt(v.$bigint) : v);

export class ProtectedStorage extends BaseStorage {
  constructor(directory, key) {
    super();
    if (!Buffer.isBuffer(key) || key.length !== 32) throw new Error('invalid_master_key');
    this.directory = directory;
    this.path = join(directory, 'session.enc');
    this.key = key;
    this.data = {};
    this.queue = Promise.resolve();
  }
  async load() {
    await mkdir(this.directory, {recursive:true, mode:0o700});
    await chmod(this.directory, 0o700);
    let bytes;
    try { bytes = await readFile(this.path); }
    catch (error) { if (error.code === 'ENOENT') return this; throw error; }
    if (bytes.length < 32 || bytes.subarray(0,4).toString() !== 'LJS1') throw new Error('invalid_encrypted_storage');
    const decipher = createDecipheriv('aes-256-gcm', this.key, bytes.subarray(4,16));
    decipher.setAAD(Buffer.from('linejs-personal-send-lab/v1'));
    decipher.setAuthTag(bytes.subarray(16,32));
    this.data = deserialize(Buffer.concat([decipher.update(bytes.subarray(32)),decipher.final()]).toString());
    return this;
  }
  async persist(data) {
    const nonce = randomBytes(12), cipher = createCipheriv('aes-256-gcm', this.key, nonce);
    cipher.setAAD(Buffer.from('linejs-personal-send-lab/v1'));
    const ciphertext = Buffer.concat([cipher.update(serialize(data)),cipher.final()]);
    const bytes = Buffer.concat([Buffer.from('LJS1'),nonce,cipher.getAuthTag(),ciphertext]);
    const temporary = this.path + '.' + randomUUID() + '.tmp';
    const file = await open(temporary, 'wx', 0o600);
    try { await file.writeFile(bytes); await file.sync(); }
    finally { await file.close(); }
    await rename(temporary, this.path);
    const folder = await open(this.directory, 'r');
    try { await folder.sync(); } finally { await folder.close(); }
  }
  mutate(operation) {
    const work = this.queue.then(async () => {
      const next = {...this.data}; operation(next);
      await this.persist(next); this.data = next;
    });
    this.queue = work.catch(() => {});
    return work;
  }
  async get(key) { await this.queue; return this.data[key]; }
  set(key,value) { return this.mutate(data => {data[key]=value;}); }
  delete(key) { return this.mutate(data => {delete data[key];}); }
  clear() { return this.mutate(data => {for (const key of Object.keys(data)) delete data[key];}); }
  async getAll() { await this.queue; return {...this.data}; }
  async migrate(storage) { for (const [key,value] of Object.entries(await this.getAll())) await storage.set(key,value); }
}
