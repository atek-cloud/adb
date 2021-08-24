import EventEmitter from 'events'
import { Readable, Transform } from 'stream'
import _debounce from 'lodash.debounce'
import { RemoteHypercore } from 'hyperspace'
import { client } from './hyperspace.js'
import Hyperbee from 'hyperbee'
import { TableSchema } from '../schemas/schema.js'
import { createValidator } from '../schemas/util.js'
import pumpify from 'pumpify'
import pump from 'pump'
import concat from 'concat-stream'
import through2 from 'through2'
import bytes from 'bytes'
import lock from '../lib/lock.js'
import { constructEntryUrl } from '../lib/strings.js'
import { TableSettings } from '../gen/atek.cloud/adb-api.js'
import { NetworkSettings } from '../gen/atek.cloud/adb-ctrl-api.js'

const READ_TIMEOUT = 10e3
const BACKGROUND_INDEXING_DELAY = 5e3 // how much time is allowed to pass before globally indexing an update
const BLOBS_RETRY_SETUP_INTERVAL = 5e3
const BLOB_CHUNK_SIZE = bytes('64kb')
const KEEP_IN_MEMORY_TTL = 15e3

interface FeedInfo {
  writable: boolean
  key: Buffer
  discoveryKey: Buffer
}

interface BeeInfo {
  writable: boolean
  discoveryKey?: Buffer
}

export interface BaseHyperbeeDBOpts {
  key?: string | Buffer
  network?: NetworkSettings
}

export interface SetupOpts {
  create?: boolean
  displayName?: string
  tables?: ({domain: string, name: string})[]
}

export interface BlobPointer {
  start: number
  end: number
  mimeType?: string
}

export interface DbRecord<T> {
  seq?: number
  key: string
  value: T | undefined | null
}

export interface DbDiff<T> {
  left: DbRecord<T> | null
  right: DbRecord<T> | null
}

export interface DbDesc {
  didFailLoad?: boolean
  displayName?: string
  blobsFeedKey?: string
  tables?: ({domain?: string, name?: string, rev?: number})[]
}

export interface TableListOpts {
  validate?: boolean
  timeout?: number
  gt?: string
  gte?: string
  lt?: string
  lte?: string
  reverse?: boolean
  limit?: number
}

export interface ReadCursor<T> {
  opts?: TableListOpts,
  db: BaseHyperbeeDB,
  next: (limit?: number) => Promise<DbRecord<T>[]|null>
}

const uwgDescription = createValidator({
  type: 'object',
  properties: {
    displayName: {type: 'string'},
    blobsFeedKey: {
      type: 'string',
      pattern: '^[a-f0-9]{64}$'
    },
    tables: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          domain: {type: 'string'},
          name: {type: 'string'},
          rev: {type: 'number'}
        }
      }
    }
  }
})

const blobPointer = createValidator({
  type: 'object',
  required: ['start', 'end', 'mimeType'],
  properties: {
    start: {type: 'number'},
    end: {type: 'number'},
    mimeType: {type: 'string'}
  }
})

export class BaseHyperbeeDB extends EventEmitter {
  key: Buffer | undefined
  network: NetworkSettings
  desc: DbDesc | undefined
  dbId: string | undefined
  bee: Hyperbee | undefined
  beeInfo: BeeInfo
  blobs: Blobs
  tables: {[schemaId: string]: Table}
  lastAccess: number
  lock: (id: string) => (Promise<() => void>)

  constructor (opts: BaseHyperbeeDBOpts = {}) {
    super()
    const key = (opts.key && typeof opts.key === 'string' ? Buffer.from(opts.key, 'hex') : opts.key) as Buffer|undefined
    this.network = {access: opts?.network?.access || 'public'}
    this.desc = undefined
    this.key = key || undefined
    this.dbId = this.key?.toString('hex')
    this.bee = undefined
    this.beeInfo = {writable: false, discoveryKey: undefined}
    this.blobs = new Blobs(this, {network: this.network})
    this.tables = {}
    this.lastAccess = 0
    this.lock = (id = '') => lock(`${this.dbId}:${id}`)
  }

  get isInMemory (): boolean {
    return !!this.bee
  }

  isEjectableFromMemory (ts: number): boolean {
    return this.isInMemory && this.lastAccess + KEEP_IN_MEMORY_TTL < ts
  }

  get writable (): boolean {
    return this.beeInfo?.writable || false
  }

  get peers () {
    return this.bee?.feed?.peers || []
  }

  get url (): string {
    return `hyper://${this.key?.toString('hex')}/`
  }

  get discoveryKey (): Buffer | undefined {
    return this.beeInfo?.discoveryKey
  }

  get displayName (): string {
    if (this.desc?.displayName) return this.desc?.displayName
    if (this.dbId) return `${this.dbId.slice(0, 6)}..${this.dbId.slice(-2)}`
    return ''
  }

  async setup (opts: SetupOpts = {}): Promise<void> {
    if (!client) {
      throw new Error('Unable to setup db: hyperspace client is not active')
    }
    if (!this.key && !opts.create) {
      throw new Error('Database instance created without key')
    }
    const release = await this.lock('setupteardown') // lock to handle multiple setup() calls
    try {
      if (this.bee) {
        return // already loaded
      }

      this.lastAccess = Date.now()
      this.bee = new Hyperbee(client.corestore().get(this.key), {
        keyEncoding: 'utf8',
        valueEncoding: 'json'
      })
      await this.bee.ready()
      this.beeInfo = {writable: this.bee.feed.writable, discoveryKey: this.bee.feed.discoveryKey}
      if (this.network.access !== 'private') {
        client.replicate(this.bee.feed)
      }

      if (!this.key) {
        this.key = this.bee.feed.key
        this.dbId = this.key?.toString('hex')
        await this.onDatabaseCreated()
        await this.updateDesc({displayName: opts.displayName, tables: opts.tables})
      }
      await this.loadDesc()
    } finally {
      release()
    }
  }

  async teardown ({unswarm} = {unswarm: false}): Promise<void> {
    const release = await this.lock('setupteardown') // lock to handle multiple teardown() calls
    try {
      if (!this.isInMemory) return
      const bee = this.bee
      this.bee = undefined
      if (this.blobs) await this.blobs.teardown({unswarm})
      if (bee && this.network.access !== 'private' && unswarm && client) {
        client.network.configure(bee.feed, {announce: false, lookup: false})
      }
      await bee?.feed?.close()
    } finally {
      release()
    }
  }

  async touch (): Promise<void> {
    this.lastAccess = Date.now()
    if (!this.isInMemory) {
      await this.setup()
    }
  }

  async loadDesc (): Promise<void> {
    const release = await this.lock(`update-db-desc`)
    try {
      if (!this.bee) throw new Error('Cannot get metadata: db is not hydrated')
      const desc = await this.bee.get('uwg', {timeout: READ_TIMEOUT})
      if (desc) {
        uwgDescription.assert(desc.value)
        this.desc = desc.value
      } else {
        this.desc = {
          didFailLoad: true,
          blobsFeedKey: undefined
        }
      }
    } finally {
      release()
    }
  }

  async updateDesc (updates: DbDesc | ((object: DbDesc) => DbDesc)): Promise<void> {
    await this.touch()
    const release = await this.lock(`update-db-desc`)
    try {
      this.desc = this.desc || {}
      if (updates) {
        if (typeof updates === 'function') {
          this.desc = updates(this.desc)
        } else {
          Object.assign(this.desc, updates)
        }
      }
      uwgDescription.assert(this.desc)
      if (!this.bee) throw new Error('Cannot write metadata: db is not hydrated')
      await this.bee.put('uwg', this.desc)
    } catch (e) {
      console.error('Failed to update database uwg record')
      console.error('Db id:', this.dbId)
      console.error('Update:', updates)
      console.error(e.toString())
      throw e
    } finally {
      release()
    }
    this.onMetaUpdated()
  }

  async onDatabaseCreated (): Promise<void> {
  }

  async onMetaUpdated (): Promise<void> {
  }

  async whenSynced (): Promise<void> {
    if (!this.bee?.feed?.writable) {
      await this.touch()
      if (!this.bee) throw new Error('Cannot update: db is not hydrated')
      await this.bee.feed.update({ifAvailable: true}).catch((e: any) => undefined)
    }
  }

  watch (_cb: (db: BaseHyperbeeDB) => void): void {
    if (!this.isInMemory || !this.writable) return
    if (!this.bee) throw new Error('Cannot watch: db is not hydrated')
    const cb = _debounce(() => _cb(this), BACKGROUND_INDEXING_DELAY, {trailing: true})
    this.bee.feed.on('append', () => cb())
  }

  table (tableId: string, definition?: TableSettings): Table {
    if (this.tables[tableId]) {
      if (!definition?.revision || definition.revision <= (this.tables[tableId].schema.revision || 0)) {
        return this.tables[tableId]
      }
    }
    if (!definition) {
      throw new Error(`Table not found and no definition was provided: ${tableId}`)
    }
    this.tables[tableId] = new Table(this, new TableSchema(tableId, definition))
    return this.tables[tableId]
  }

  async ensureTableIsInMetadata (schema: TableSchema) {
    // TODO
    // if (this.desc.tables?.find?.(t => t.domain === schema.domain && t.name === schema.name && t.rev >= schema.rev)) {
    //   return
    // }
    // await this.updateDesc(desc => {
    //   desc.tables = desc.tables || []
    //   const existing = desc.tables.find(t => t.domain === schema.domain && t.name === schema.name)
    //   if (existing) {
    //     existing.rev = schema.rev
    //   } else {
    //     desc.tables.push({domain: schema.domain, name: schema.name, rev: schema.rev})
    //   }
    //   return desc
    // })
  }
}

class Blobs {
  db: BaseHyperbeeDB
  feed: RemoteHypercore | undefined
  feedInfo: FeedInfo | undefined
  network: NetworkSettings

  constructor (db: BaseHyperbeeDB, {network}: {network:  NetworkSettings}) {
    this.db = db
    this.feed = undefined
    this.feedInfo = undefined
    this.network = network
  }

  get writable (): boolean {
    return this.feedInfo?.writable || false
  }

  get peers () {
    return this.feed?.peers || []
  }

  get key (): Buffer | undefined {
    return this.feedInfo?.key
  }

  get discoveryKey (): Buffer | undefined {
    return this.feedInfo?.discoveryKey
  }

  async setup (): Promise<void> {
    if (this.feed) {
      return // already setup
    }
    if (!client) {
      throw new Error('Failed to setup blobs: hyperspace client not active')
    }
    if (this.db.desc?.didFailLoad && !this.writable) {
      console.log('Failed to load database description for external database', this.db.dbId, '- periodically retrying')
      return this.periodicallyRetrySetup()
    } else if (!this.db.desc?.blobsFeedKey) {
      if (this.db.writable) {
        this.feed = client.corestore().get(null)
        await this.feed.ready()
        this.feedInfo = {writable: this.feed.writable, key: this.feed.key, discoveryKey: this.feed.discoveryKey}
        await this.db.updateDesc({blobsFeedKey: this.feed.key.toString('hex')})
      }
    } else {
      this.feed = client.corestore().get(Buffer.from(this.db.desc?.blobsFeedKey, 'hex'))
      await this.feed.ready()
      this.feedInfo = {writable: this.feed.writable, key: this.feed.key, discoveryKey: this.feed.discoveryKey}
    }
    if (this.feed && this.network.access !== 'private') {
      client.replicate(this.feed)
    }
  }

  async periodicallyRetrySetup (): Promise<void> {
    // this function is called when a remote bee fails to load a db description
    if (!this.db.isInMemory) return
    await this.db.loadDesc()
    if (this.db.desc && !this.db.desc.didFailLoad) {
      console.log('Resolved missing database description for external database', this.db.dbId)
      await this.setup()
      return
    }
    setTimeout(() => this.periodicallyRetrySetup(), BLOBS_RETRY_SETUP_INTERVAL)
  }

  teardown ({unswarm} = {unswarm: false}): void {
    if (!this.feed) return
    if (this.network.access !== 'private' && unswarm && client) {
      client.network.configure(this.feed, {announce: false, lookup: false})
    }
    this.feed = undefined
  }

  async createReadStream (pointerValue: BlobPointer): Promise<Readable> {
    if (!this.feed) throw new Error('Unable to get blob: blob feed not initialized')
    await this.db.touch()
    return this.feed.createReadStream({
      start: pointerValue.start,
      end: pointerValue.end,
      timeout: READ_TIMEOUT
    })
  }

  async get (pointerValue: BlobPointer): Promise<Buffer> {
    const stream = await this.createReadStream(pointerValue)
    return new Promise((resolve, reject) => {
      pump(
        stream,
        concat({encoding: 'buffer'}, resolve),
        reject
      )
    })
  }

  download (pointerValue: BlobPointer) {
    if (!this.feed) throw new Error('Unable to download blob: blob feed not initialized')
    return this.feed.download(pointerValue.start, pointerValue.end)
  }

  async isCached (pointerValue: BlobPointer): Promise<boolean> {
    if (!this.feed) throw new Error('Unable to read blob: blob feed not initialized')
    for (let i = pointerValue.start; i <= pointerValue.end; i++) {
      if (!(await this.feed.has(i))) return false
    }
    return true
  }

  async put (buf: Buffer): Promise<BlobPointer> {
    if (!this.feed) throw new Error('Unable to put blob: blob feed not initialized')
    const chunks = chunkify(buf, BLOB_CHUNK_SIZE)
    const start = await this.feed.append(chunks)
    return {start, end: start + chunks.length}
  }

  async decache (pointerValue: BlobPointer) {
    // TODO hyperspace needs to export a clear command to do this
    // await this.feed.clear(pointerValue.start, pointerValue.end)
  }
}

export class Table {
  db: BaseHyperbeeDB
  _bee: Hyperbee | undefined
  schema: TableSchema
  _schemaDomain: string
  _schemaName: string
  lock: (id: string) => (Promise<() => void>)

  constructor (db: BaseHyperbeeDB, schema: TableSchema) {
    const [domain, name] = schema.tableId.split('/')
    this.db = db
    this._bee = undefined
    this.schema = schema
    this._schemaDomain = domain
    this._schemaName = name
    this.lock = (id = '') => this.db.lock(`${this.schema.tableId}:${id}`)
  }

  teardown () {
  }

  get bee (): Hyperbee {
    if (!this.db.bee) throw new Error('Cannot access database: db is not hydrated')
    if (!this._bee || this._bee.feed !== this.db.bee?.feed) {
      // bee was unloaded since last cache, recreate from current bee
      this._bee = this.db.bee.sub(this._schemaDomain).sub(this._schemaName)
    }
    return this._bee
  }

  getBlobsSub (key: string): Hyperbee {
    return this.bee.sub(key).sub('blobs')
  }

  constructBeeKey (key: string): Buffer {
    return this.bee.keyEncoding.encode(key)
  }

  constructEntryUrl (key: string): string {
    return constructEntryUrl(this.db.url, this.schema.tableId, key)
  }

  async get<T> (key: string): Promise<DbRecord<T>> {
    await this.db.touch()
    const entry = await this.bee.get(String(key), {timeout: READ_TIMEOUT})
    if (entry) {
      this.schema.assertValid(entry.value)
    }
    return entry
  }

  async listBlobPointers (key: string): Promise<DbRecord<BlobPointer>[]> {
    await this.db.touch()
    return new Promise((resolve, reject) => {
      pump(
        this.getBlobsSub(key).createReadStream({timeout: READ_TIMEOUT}),
        through2.obj(function (this: Transform, entry, enc, cb) {
          const valid = blobPointer.validate(entry.value)
          if (valid) this.push(entry)
          cb()
        }),
        concat(resolve),
        (err: any) => {
          if (err) reject(err)
        }
      )
    })
  }

  async getBlobPointer (key: string, blobName: string): Promise<DbRecord<BlobPointer>> {
    await this.db.touch()
    const pointer = await this.getBlobsSub(key).get(blobName)
    if (!pointer) throw new Error('Blob not found')
    this.schema.assertBlobMimeTypeValid(blobName, pointer.value.mimeType)
    blobPointer.assert(pointer.value)
    return pointer
  }

  async isBlobCached (key: string, blobNameOrPointer: string|DbRecord<BlobPointer>): Promise<boolean> {
    let pointer = blobNameOrPointer
    if (typeof pointer === 'string') {
      pointer = await this.getBlobPointer(key, pointer)
    }
    if (!pointer.value) throw new Error('Blob not found')
    return this.db.blobs.isCached(pointer.value)
  }

  async getBlob (key: string, blobNameOrPointer: string|DbRecord<BlobPointer>, encoding: BufferEncoding|undefined = undefined): Promise<{mimeType?: string, buf: Buffer|string}> {
    let pointer = blobNameOrPointer
    if (typeof pointer === 'string') {
      pointer = await this.getBlobPointer(key, pointer)
    }
    if (!pointer.value) throw new Error('Blob not found')
    const buf = await this.db.blobs.get(pointer.value)
    if (typeof blobNameOrPointer === 'string') {
      this.schema.assertBlobSizeValid(blobNameOrPointer, buf.length)
    }
    return {
      mimeType: pointer.value.mimeType,
      buf: encoding && encoding !== 'binary' ? buf.toString(encoding) : buf
    }
  }

  async createBlobReadStream (key: string, blobNameOrPointer: string|DbRecord<BlobPointer>): Promise<Readable> {
    let pointer = blobNameOrPointer
    if (typeof pointer === 'string') {
      pointer = await this.getBlobPointer(key, pointer)
    }
    if (!pointer.value) throw new Error('Blob not found')
    return this.db.blobs.createReadStream(pointer.value)
  }

  async downloadBlobs (key: string): Promise<void> {
    await this.db.touch()
    const pointers = await this.listBlobPointers(key)
    if (!pointers?.length) return
    for (const pointer of pointers) {
      if (!pointer.value) continue
      try {
        this.schema.assertBlobMimeTypeValid(pointer.key, pointer.value.mimeType)
        blobPointer.assert(pointer.value)
        await this.db.blobs.download(pointer.value)
      } catch (e) {}
    }
  }

  async put (key: string, value: any): Promise<void> {
    await this.db.touch()
    this.schema.assertValid(value)
    await this.db.ensureTableIsInMetadata(this.schema)
    const res = await this.bee.put(String(key), value)
    return res
  }

  async putBlob (key: string, blobName: string, buf: Buffer, {mimeType}: {mimeType?: string}): Promise<void> {
    await this.db.touch()
    this.schema.assertBlobMimeTypeValid(blobName, mimeType)
    this.schema.assertBlobSizeValid(blobName, buf.length)
    const pointerValue = await this.db.blobs.put(buf)
    pointerValue.mimeType = mimeType
    blobPointer.assert(pointerValue)
    await this.db.ensureTableIsInMetadata(this.schema)
    await this.getBlobsSub(key).put(blobName, pointerValue)
  }

  async del (key: string): Promise<void> {
    await this.db.touch()
    const res = await this.bee.del(String(key))
    /* dont await */ this.delAllBlobs(String(key))
    return res
  }

  async delBlob (key: string, blobName: string): Promise<void> {
    blobName = String(blobName)
    const pointer = await this.getBlobPointer(key, blobName)
    if (pointer.value) await this.db.blobs.decache(pointer.value)
    await this.getBlobsSub(key).del(blobName)
  }

  async delAllBlobs (key: string): Promise<void> {
    const pointers = await this.listBlobPointers(key)
    for (const pointer of pointers) {
      if (pointer.value) await this.db.blobs.decache(pointer.value)
      await this.getBlobsSub(key).del(pointer.key)
    }
  }

  async createReadStream (opts?: TableListOpts): Promise<Readable> {
    await this.db.touch()
    const _this = this
    opts = opts || {}
    opts.timeout = READ_TIMEOUT
    return pumpify.obj(
      this.bee.createReadStream(opts),
      through2.obj(function (entry, enc, cb) {
        if (opts?.validate === false) {
          this.push(entry)
        } else {
          const valid = _this.schema.validate(entry.value)
          if (valid) this.push(entry)
        }
        cb()
      })
    )
  }

  async list<T> (opts?: TableListOpts): Promise<DbRecord<T>[]> {
    // no need to .touch() because createReadStream() does it
    opts = opts || {}
    opts.timeout = READ_TIMEOUT
    const stream = await this.createReadStream(opts)
    return new Promise((resolve, reject) => {
      pump(
        stream,
        concat(resolve),
        (err: any) => {
          if (err) reject(err)
        }
      )
    })
  }

  async scanFind<T> (opts: TableListOpts, fn: (record: DbRecord<T>) => boolean): Promise<DbRecord<T>|undefined> {
    // no need to .touch() because createReadStream() does it
    const rs = await this.createReadStream(opts)
    return new Promise((resolve, reject) => {
      let found = false
      opts = opts || {}
      opts.timeout = READ_TIMEOUT
      rs.on('data', record => {
        if (found) return
        if (fn(record)) {
          // TODO fix rs.destroy()
          // rs.destroy()
          found = true
          resolve(record)
        }
      })
      rs.on('error', (e) => reject(e))
      rs.on('end', () => {
        if (!found) resolve(undefined)
      })
    })
  }

  cursorRead<T> (opts?: TableListOpts): ReadCursor<T> {
    // no need to .touch() because list() does it
    let lt = opts?.lt
    let atEnd = false
    return {
      opts,
      db: this.db,
      next: async (limit?: number): Promise<DbRecord<T>[]|null> => {
        if (atEnd) return null
        const res = await this.list<T>(Object.assign({}, opts, {lt, limit})).catch(e => [])
        if (res.length === 0) {
          atEnd = true
          return null
        }
        lt = res[res.length - 1].key
        return res
      }
    }
  }

  async listDiff<T> (other: number): Promise<DbDiff<T>[]> {
    if (!this.db.bee) throw new Error('Cannot listDiff: db is not hydrated')
    await this.db.touch()
    /**
     * HACK
     * There's a bug in Hyperbee where createDiffStream() breaks on sub()s.
     * We have to run it without using sub() and then filter the results.
     * -prf
     */
    // const co = this.db.bee.checkout(other).sub(this._schemaDomain).sub(this._schemaName)
    // return new Promise((resolve, reject) => {
    //   pump(
    //     co.createDiffStream(this.bee.version),
    //     concat(resolve),
    //     (err: any) => {
    //       pend()
    //       if (err) reject(err)
    //     }
    //   )
    // })
    const co = this.db.bee.checkout(other)
    const diffs = (await new Promise((resolve, reject) => {
      pump(
        co.createDiffStream(this.bee.version),
        concat(resolve),
        (err: any) => {
          if (err) reject(err)
        }
      )
    })) as DbDiff<T>[]
    const prefix = `${this._schemaDomain}\x00${this._schemaName}\x00`
    return diffs.filter(diff => {
      const key = (diff?.right||diff?.left||{}).key
      if (key?.startsWith(prefix)) {
        if (diff.left) diff.left.key = diff.left.key.slice(prefix.length)
        if (diff.right) diff.right.key = diff.right.key.slice(prefix.length)
        return true
      }
      return false
    })
  }
}

function chunkify (buf: Buffer, chunkSize: number): Buffer[] {
  const chunks = []
  while (buf.length) {
    chunks.push(buf.slice(0, chunkSize))
    buf = buf.slice(chunkSize)
  }
  return chunks
}
