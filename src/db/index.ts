import * as hyperspace from './hyperspace.js'
import { BaseHyperbeeDB, SetupOpts, NetworkSettings, Table } from './base.js'
import * as schemas from '../schemas/index.js'
import { HYPER_KEY, hyperUrlToKeyStr } from '../lib/strings.js'
import { InvalidIdError } from '../lib/errors.js'
import { CaseInsensitiveMap } from '../lib/map.js'
import lock from '../lib/lock.js'

const SWEEP_INACTIVE_DBS_INTERVAL = 10e3

interface DatabaseRecordAccess {
  appId?: string
  alias?: string
  persist?: boolean
  presync?: boolean
}

interface DatabaseRecord {
  key: string
  value: {
    dbId: string
    cachedMeta?: {
      displayName?: string
      writable?: boolean
    }
    network?: NetworkSettings
    access?: DatabaseRecordAccess[]
    createdBy?: {
      username?: string
      appId?: string
    },
    createdAt?: string
  }
}

interface CloudAppDb {
  dbId: string // The database identifier.
  displayName?: string // The user-friendly name of the database.
  writable?: boolean // Is the database writable?
  alias?: string // The alias ID of this database for this application.
  persist: boolean // Does this application want to keep the database in storage?
  presync: boolean // Does this application want the database to be fetched optimistically from the network?
}

enum DbInternalType {
  HYPERBEE = 'hyperbee'
}

interface DbSettings {
  type?: DbInternalType
  alias?: string // An alias ID for the application to reference the database.
  displayName?: string // The database's display name.
  tables?: string[] // The database's initial configured tables.
  network?: NetworkSettings // The database's network settings.
}

// exported api
// =

export let privateServerDb: PrivateServerDB | undefined = undefined
export const dbs = new CaseInsensitiveMap<PrivateServerDB|GeneralDB>()


// Initialize the database system. Must be called during setup.
export async function setup(): Promise<void> {
  await hyperspace.setup()
  await schemas.setup()
  
  privateServerDb = new PrivateServerDB({key: process.env.ATEK_SERVER_DBID})
  await privateServerDb.setup({create: !process.env.ATEK_SERVER_DBID})
  if (privateServerDb.dbId) {
    dbs.set(privateServerDb.dbId, privateServerDb)
  }

  // TODO- do we need to optimistically load databases?

  const sweepInterval = setInterval(sweepInactiveDbs, SWEEP_INACTIVE_DBS_INTERVAL)
  sweepInterval.unref()
}

// Shut down the database system. Must be called during close.
export async function cleanup (): Promise<void> {
  await hyperspace.cleanup()
}

// Get a database that's in memory.
// NOTE: use this sparingly. It's better to use `loadDb()` in order to correctly track database accesses by applications.
export function getDb (dbId: string): PrivateServerDB|GeneralDB|undefined {
  return dbs.get(normalizeDbId(dbId))
}

// List all databases in memory.
// NOTE: use this sparingly. It's better to use `loadDb()` in order to correctly track database accesses by applications.
export function getAllDbs (): (PrivateServerDB|GeneralDB)[] {
  return Array.from(dbs.values())
}

// Get a database that's in memory using the discovery key.
// NOTE: use this sparingly. It's better to use `loadDb()` in order to correctly track database accesses by applications.
export function getDbByDkey (dkey: string): PrivateServerDB|GeneralDB|undefined {
  if (privateServerDb?.discoveryKey?.toString('hex') === dkey) return privateServerDb
  for (const db of dbs.values()) {
    if (db.discoveryKey?.toString('hex') === dkey) return db
  }
}

// Get or load a database.
// The access settings by `appId` is tracked automatically in the database.
export async function loadDb (appId: string, dbId: string): Promise<PrivateServerDB|GeneralDB|undefined> {
  if (!privateServerDb || !privateServerDb.databases) {
    throw new Error('Cannot resolve alias: server db not available')
  }
  if (!appId || !dbId) throw new Error(`Must provide appId and dbId to loadDb()`)
  dbId = normalizeDbId(dbId)
  const release = await lock(`load-db:${dbId}`)
  try {
    const dbRecord = (await privateServerDb.databases.get(dbId)) as DatabaseRecord
    let db = getDb(dbId)
    if (!db) {
      db = new GeneralDB({
        key: dbId,
        network: dbRecord.value?.network || {access: 'public'}
      })
      await db.setup({create: false})
      dbs.set(dbId, db)
    }
    if (appId !== 'system' && !dbRecord?.value?.access?.find(a => a.appId === appId)) {
      await privateServerDb.configureAppDbAccess(appId, dbId, {persist: false})
    }
    return db
  } catch (e) {
    console.error('Failed to load database dbId:', dbId)
    console.error(e)
    return undefined
  } finally {
    release()
  }
}

// Resolves an app's db alias to its dbId
export async function resolveAlias (appId: string, alias: string): Promise<string|undefined> {
  if (!privateServerDb || !privateServerDb.databases) {
    throw new Error('Cannot resolve alias: server db not available')
  }
  if (HYPER_KEY.test(alias)) return alias
  const dbRecords = (await privateServerDb.databases.list()) as DatabaseRecord[]
  const dbRecord = dbRecords.find(r => r.value.access?.find(a => a.appId === appId && a.alias === alias))
  if (dbRecord) {
    return dbRecord.value.dbId
  }
}

export async function createDb (appId: string, opts: DbSettings): Promise<GeneralDB> {
  if (!privateServerDb) {
    throw new Error('Cannot create new db: server db not available')
  }
  const netAccess = opts.network?.access || 'public'
  const db = new GeneralDB({network: {access: netAccess}})
  await db.setup({
    create: true,
    displayName: opts.displayName,
    tables: (opts.tables || []).map(table => {
      const [domain, name] = table.split('/')
      return {domain, name}
    }),
  })
  if (db.dbId) {
    dbs.set(db.dbId, db)
    await privateServerDb.updateDbRecord(db.dbId, dbRecord => {
      dbRecord.value.network = {access: netAccess}
      dbRecord.value.access = [{appId, alias: opts.alias, persist: true}]
      dbRecord.value.createdBy = {appId}
      dbRecord.value.createdAt = (new Date()).toISOString()
      return true
    })
  }
  return db
}

// Get a database by an application's alias ID. If a db does not exist at that alias, this function will create one.
export async function getOrCreateDbByAlias (appId: string, alias: string, settings: DbSettings): Promise<PrivateServerDB|GeneralDB> {
  const release = await lock(`get-or-create-db:${alias}`)
  try {
    const dbId = await resolveAlias(appId, alias)
    if (dbId) {
      const db = await loadDb(appId, dbId)
      if (!db) throw new Error(`Failed to load database: ${alias} (${dbId})`)
      return db
    } else {
      return createDb(appId, Object.assign({}, settings, {alias}))
    }
  } finally {
    release()
  }
}

// List databases attached to an application.
export function listAppDbs (appId: string): Promise<CloudAppDb[]> {
  if (!privateServerDb) {
    throw new Error('Cannot list app db records: server db not available')
  }
  return privateServerDb.listAppDbs(appId)
}

// Update the configuration of a database's attachment to an application.
export async function configureAppDbAccess (appId: string, dbId: string, config: {persist?: boolean, presync?: boolean} = {}): Promise<DatabaseRecord> {
  if (!privateServerDb) {
    throw new Error('Cannot configure app db access: server db not available')
  }
  if (!HYPER_KEY.test(dbId)) {
    const resolvedDbId = await resolveAlias(appId, dbId)
    if (!resolvedDbId) throw new Error(`Invalid database ID: ${dbId}`)
    dbId = resolvedDbId
  }
  return privateServerDb.configureAppDbAccess(appId, dbId, config)
}

// Waits for all databases to finish their sync events.
// NOTE: this method should only be used for tests
export async function whenAllSynced (): Promise<void> {
  for (let db of getAllDbs()) {
    await db.whenSynced()
  }
}

// Checks whether a blob is locally available.
// TODO May no longer be needed, or may need to be updated.
// export async function isRecordBlobCached (dbUrl: string, blobName: string): Promise<boolean> {
//   const urlp = new URL(dbUrl)
//   const db = dbs.get(urlp.hostname)
//   const pathParts = urlp.pathname.split('/').filter(Boolean)
//   const table = db.getTable(`${pathParts[0]}/${pathParts[1]}`)
//   return await table.isBlobCached(pathParts[2], blobName)
// }

// internal methods
// =

// Ensure that dbId is a valid identifer and normalize it to the hex-string key.
function normalizeDbId (dbId: string): string {
  if (!dbId || typeof dbId !== 'string') throw new InvalidIdError(`Invalid database ID: ${dbId}`)
  if (dbId.startsWith('hyper://')) {
    dbId = hyperUrlToKeyStr(dbId) || dbId
  }
  if (!HYPER_KEY.test(dbId)) throw new InvalidIdError(`Invalid database ID: ${dbId}`)
  return dbId
}

// Looks for any databases which are not in use and unloads them.
async function sweepInactiveDbs (): Promise<void> {
  const ts = Date.now()
  for (const db of getAllDbs()) {
    if (db.isEjectableFromMemory(ts)) {
      await db.teardown({unswarm: false})
    }
  }
}

class PrivateServerDB extends BaseHyperbeeDB {
  accounts: Table | undefined
  accountSessions: Table |  undefined
  apps: Table |  undefined
  appProfileSessions: Table |  undefined
  databases: Table |  undefined

  constructor ({key}: {key: string|undefined}) {
    super({
      key,
      network: {access: 'private'}
    })
  }

  isEjectableFromMemory (ts: number) {
    return false // never eject the private server db from memory
  }

  async setup (opts: SetupOpts) {
    await super.setup(opts)
    this.accounts = this.getTable('ctzn.network/account')
    this.accountSessions = this.getTable('ctzn.network/account-session')
    this.apps = this.getTable('ctzn.network/application')
    this.appProfileSessions = this.getTable('ctzn.network/app-profile-session')
    this.databases = this.getTable('ctzn.network/database')
  }
  
  async onDatabaseCreated () {
    console.log('New private server database created, key:', this.dbId)
    await this.updateDesc({displayName: 'Server Registry'})
  }

  // List databases attached to an application.
  async listAppDbs (appId: string): Promise<CloudAppDb[]> {
    if (!this.databases) throw new Error('Cannot list app db record: this database is not setup')
    const databases: CloudAppDb[] = []
    const dbRecords = (await this.databases.list()) as DatabaseRecord[]
    for (const dbRecord of dbRecords) {
      const access = dbRecord.value.access?.find(a => a.appId === appId)
      if (access) {
        databases.push({
          dbId: dbRecord.value.dbId,
          displayName: dbRecord.value.cachedMeta?.displayName,
          writable: dbRecord.value.cachedMeta?.writable,
          alias: access.alias,
          persist: access.persist || false,
          presync: access.presync || false
        })
      }
    }
    return databases
  }

  // Update a database's record.
  async updateDbRecord (dbId: string, updateFn: (record: DatabaseRecord) => boolean): Promise<DatabaseRecord> {
    if (!this.databases) throw new Error('Cannot update db record: this database is not setup')
    dbId = normalizeDbId(dbId)
    const release = await this.databases.lock(dbId)
    try {
      let isNew = false
      let dbRecord = (await this.databases.get(dbId)) as DatabaseRecord
      if (!dbRecord) {
        const db = await loadDb('system', dbId)
        if (!db) throw new Error(`Failed to load database: ${dbId}`)
        isNew = true
        dbRecord = {
          key: dbId,
          value: {
            dbId,
            cachedMeta: {
              displayName: db.displayName,
              writable: db.writable
            },
            access: [],
            createdBy: {appId: '', username: ''},
            createdAt: ''
          }
        }
      }
      const wasChanged = updateFn(dbRecord)
      if (wasChanged !== false || isNew) {
        await this.databases.put(dbRecord.key, dbRecord.value)
      }
      return dbRecord
    } finally {
      release()
    }
  }

  // Update a database record's cached metadata.
  updateDbRecordCachedMeta (db: PrivateServerDB|GeneralDB|BaseHyperbeeDB): Promise<DatabaseRecord> {
    if (!db.dbId) throw new Error('Cannot update record cached meta: database not hydrated')
    return this.updateDbRecord(db.dbId, dbRecord => {
      if (!dbRecord.value.cachedMeta) {
        dbRecord.value.cachedMeta = {}
      }
      if (
        (dbRecord.value.cachedMeta.displayName === db.displayName)
        && (dbRecord.value.cachedMeta.writable === db.writable)
      ) {
        return false
      }
      dbRecord.value.cachedMeta.displayName = db.displayName
      dbRecord.value.cachedMeta.writable = db.writable
      return true
    })
  }

  // Update the configuration of a database's attachment to an application.
  configureAppDbAccess (appId: string, dbId: string, config: DatabaseRecordAccess = {}): Promise<DatabaseRecord> {
    return this.updateDbRecord(dbId, dbRecord => {
      if (!dbRecord.value.access) dbRecord.value.access = []
      let access = dbRecord.value.access.find(a => a.appId === appId)
      if (!access) {
        access = {
          appId,
          persist: false,
          presync: false
        }
        dbRecord.value.access.push(access)
      }

      if (typeof config.persist === 'boolean') access.persist = config.persist
      if (typeof config.presync === 'boolean') access.presync = config.presync
      return true
    })
  }
}

class GeneralDB extends BaseHyperbeeDB {
  async setup (opts: SetupOpts) {
    await super.setup(opts)
    await this.blobs.setup()
  }

  async onMetaUpdated () {
    try {
      await privateServerDb?.updateDbRecordCachedMeta(this)
    } catch (e) {
      console.error('Failed to update cached metadata for db')
      console.error('Database ID:', this.dbId)
      console.error(e)
    }
  }
}
