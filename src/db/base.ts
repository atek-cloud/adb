import EventEmitter from 'events'
import { Readable } from 'stream'
import _debounce from 'lodash.debounce'
import { client } from './hyperspace.js'
import Hyperbee from 'hyperbee'
import pump from 'pump'
import concat from 'concat-stream'
import lock from '../lib/lock.js'
import { Database, DatabaseAccess } from './schemas.js'

const READ_TIMEOUT = 10e3
const BACKGROUND_INDEXING_DELAY = 5e3 // how much time is allowed to pass before globally indexing an update
const KEEP_IN_MEMORY_TTL = 15e3

interface BeeInfo {
  writable: boolean
  discoveryKey?: Buffer
}

export interface BaseHyperbeeDBOpts {
  key?: string | Buffer
  access?: DatabaseAccess
}

export interface SetupOpts {
  create?: boolean
  tables?: ({domain: string, name: string})[]
}

export interface DbRecord<T> {
  key: string
  seq?: number
  value: T
}

export interface DbDiff<T> {
  left: DbRecord<T> | null
  right: DbRecord<T> | null
}

export interface DbDesc {
  didFailLoad?: boolean
  blobsFeedKey?: string
}

export interface ListOpts {
  timeout?: number
  gt?: string
  gte?: string
  lt?: string
  lte?: string
  reverse?: boolean
  limit?: number
}

export interface ReadCursor<T> {
  opts?: ListOpts,
  db: BaseHyperbeeDB,
  next: (limit?: number) => Promise<DbRecord<T>[]|null>
}

export class BaseHyperbeeDB extends EventEmitter {
  key: Buffer | undefined
  access: DatabaseAccess
  desc: DbDesc | undefined
  dbId: string | undefined
  bee: Hyperbee | undefined
  beeInfo: BeeInfo
  lastAccess: number
  lock: (id: string) => (Promise<() => void>)

  constructor (opts: BaseHyperbeeDBOpts = {}) {
    super()
    const key = (opts.key && typeof opts.key === 'string' ? Buffer.from(opts.key, 'hex') : opts.key) as Buffer|undefined
    this.access = (opts.access || 'public') as DatabaseAccess
    this.desc = undefined
    this.key = key || undefined
    this.dbId = this.key?.toString('hex')
    this.bee = undefined
    this.beeInfo = {writable: false, discoveryKey: undefined}
    this.lastAccess = 0
    this.lock = (id = '') => lock(`${this.dbId}:${id}`)
  }

  lockPath (path: string[]) {
    return this.lock(`/${path.join('/')}`)
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
      if (this.access !== DatabaseAccess.private) {
        client.replicate(this.bee.feed)
      }

      if (!this.key) {
        this.key = this.bee.feed.key
        this.dbId = this.key?.toString('hex')
        await this.onDatabaseCreated()
      }
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
      if (bee && this.access !== DatabaseAccess.private && unswarm && client) {
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

  async onDatabaseCreated (): Promise<void> {
  }

  async onMetaUpdated (): Promise<void> {
  }

  onConfigUpdated (dbValue: Database): void {
    if (this.access !== dbValue.access) {
      this.access = (dbValue.access as DatabaseAccess)
      if (!client || !this.key) return
      if (this.access === 'private') {
        console.log('Unswarming', this.dbId, 'due to config change')
        client.network.configure(this.key, {announce: false, lookup: false})
      } else {
        console.log('Swarming', this.dbId, 'due to config change')
        client.network.configure(this.key, {announce: true, lookup: true})
      }
    }
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

  subAtPath (path: string|string[]): {bee?: Hyperbee} {
    const parts = Array.isArray(path) ? path : path?.split?.('/')?.filter(Boolean)
    if (!parts) throw new Error(`Invalid path: ${path}`)
    let bee = this.bee
    for (let i = 0; i < parts.length; i++) {
      bee = bee?.sub(parts[i])
    }
    return {bee}
  }

  keyAtPath (path: string|string[]): {bee?: Hyperbee, key: string} {
    const parts = Array.isArray(path) ? path : path?.split?.('/')?.filter(Boolean)
    if (!parts) throw new Error(`Invalid path: ${path}`)
    let bee = this.bee
    for (let i = 0; i < parts.length - 1; i++) {
      bee = bee?.sub(parts[i])
    }
    return {bee, key: parts[parts.length - 1]}
  }

  async get<T> (path: string|string[]): Promise<DbRecord<T>> {
    await this.touch()
    const {bee, key} = this.keyAtPath(path)
    return bee?.get(String(key), {timeout: READ_TIMEOUT})
  }

  async put (path: string|string[], value: any): Promise<void> {
    await this.touch()
    const {bee, key} = this.keyAtPath(path)
    return bee?.put(String(key), value)
  }

  async del (path: string|string[]): Promise<void> {
    await this.touch()
    const {bee, key} = this.keyAtPath(path)
    return bee?.del(String(key))
  }

  async createReadStream (path: string|string[], opts?: ListOpts): Promise<Readable> {
    await this.touch()
    opts = opts || {}
    opts.timeout = READ_TIMEOUT
    const {bee} = this.subAtPath(path)
    return bee?.createReadStream(opts)
  }

  async list<T> (path: string|string[], opts?: ListOpts): Promise<DbRecord<T>[]> {
    // no need to .touch() because createReadStream() does it
    opts = opts || {}
    opts.timeout = READ_TIMEOUT
    // return listShallow(this.bee, path)
    const stream = await this.createReadStream(path, opts)
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

  async scanFind<T> (path: string|string[], opts: ListOpts, fn: (record: DbRecord<T>) => boolean): Promise<DbRecord<T>|undefined> {
    // no need to .touch() because createReadStream() does it
    const rs = await this.createReadStream(path, opts)
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

  cursorRead<T> (path: string|string[], opts?: ListOpts): ReadCursor<T> {
    // no need to .touch() because list() does it
    let lt = opts?.lt
    let atEnd = false
    return {
      opts,
      db: this,
      next: async (limit?: number): Promise<DbRecord<T>[]|null> => {
        if (atEnd) return null
        const res = await this.list<T>(path, Object.assign({}, opts, {lt, limit})).catch(e => [])
        if (res.length === 0) {
          atEnd = true
          return null
        }
        lt = res[res.length - 1].key
        return res
      }
    }
  }
}

/*
const SEP = '\x00'
const MIN = SEP
const MAX = Buffer.from([255]).toString('utf8')

async function listShallow<T> (bee: Hyperbee|undefined, path: string|string[]): Promise<DbRecord<T>[]> {
  if (!bee) return []
  if (typeof path === 'string') {
    path = path.split('/').filter(Boolean)
  }

  var arr: DbRecord<T>[] = []
  var pathlen = path && path.length > 0 ? path.length : 0
  var bot = path && path.length > 0 ? pathToKey([...path, MIN]) : MIN
  var top = path && path.length > 0 ? pathToKey([...path, MAX]) : MAX
  console.log('listShallow', {path, bot, top})
  do {
    console.log('peek', {bot, top})
    const item = await bee.peek({gt: bot, lt: top})
    if (!item) return arr

    const itemPath = keyToPath(item.key)
    if (itemPath.length > pathlen + 1) {
      const containerPath = itemPath.slice(0, -1)
      console.log('container hit', {containerPath, bot, top})
      arr.push({seq: undefined, key: itemPath[containerPath.length - 1], path: `/${containerPath.join('/')}`, hasChildren: true, value: undefined})
      bot = pathToKey([...containerPath, MAX])
    } else {
      console.log('item hit', {itemPath, bot, top})
      arr.push({seq: item.seq, key: itemPath[itemPath.length - 1], path: `/${itemPath.join('/')}`, hasChildren: false, value: item.value})
      bot = pathToKey(itemPath)
    }
  } while (true)
}

function keyToPath (key: string): string[] {
  return key.split(SEP)
}

function pathToKey (segments: string[]): string {
  return segments.join(SEP)
}*/