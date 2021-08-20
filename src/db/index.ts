import * as hyperspace from './hyperspace.js'
import { BaseHyperbeeDB, SetupOpts, NetworkSettings, DbRecord } from './base.js'
import { PrivateServerDB, ServiceDbConfig } from './private-server-db.js'
import { HYPER_KEY, normalizeDbId } from '../lib/strings.js'
import { CaseInsensitiveMap } from '../lib/map.js'
import lock from '../lib/lock.js'

import DatabaseRecordValue, { NetworkAccess } from '../gen/atek.cloud/database.js'

const SWEEP_INACTIVE_DBS_INTERVAL = 10e3

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
  
  if (process.env.ATEK_SERVER_DBID) {
    privateServerDb = new PrivateServerDB({key: process.env.ATEK_SERVER_DBID})
    await privateServerDb.setup({create: false})
  } else if (process.env.ATEK_SERVER_DB_CREATE_NEW) {
    privateServerDb = new PrivateServerDB({key: undefined})
    await privateServerDb.setup({create: true})
  } else {
    throw new Error('No server database instructions provided. Please set ATEK_SERVER_DBID or ATEK_SERVER_DB_CREATE_NEW env variables.')
  }
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
// The access settings by `serviceId` is tracked automatically in the database.
export async function loadDb (serviceId: string, dbId: string): Promise<PrivateServerDB|GeneralDB> {
  if (!privateServerDb || !privateServerDb.databases) {
    throw new Error('Cannot resolve alias: server db not available')
  }
  if (!serviceId || !dbId) throw new Error(`Must provide serviceId and dbId to loadDb()`)
  dbId = normalizeDbId(dbId)
  const release = await lock(`load-db:${dbId}`)
  try {
    const dbRecord = (await privateServerDb.databases.get<DatabaseRecordValue>(dbId))
    let db = getDb(dbId)
    if (!db) {
      db = new GeneralDB({
        key: dbId,
        network: dbRecord.value?.network || {access: 'public'}
      })
      await db.setup({create: false})
      dbs.set(dbId, db)
    }
    if (serviceId !== 'system' && !dbRecord?.value?.services?.find(a => a.serviceId === serviceId)) {
      await privateServerDb.configureServiceDbAccess(serviceId, dbId, {persist: false})
    }
    return db
  } catch (e) {
    console.error('Failed to load database dbId:', dbId)
    console.error(e)
    throw `Failed to load database dbId: ${dbId}`
  } finally {
    release()
  }
}

// Resolves an app's db alias to its dbId
export async function resolveAlias (serviceId: string, alias: string): Promise<string|undefined> {
  if (!privateServerDb || !privateServerDb.databases) {
    throw new Error('Cannot resolve alias: server db not available')
  }
  if (HYPER_KEY.test(alias)) return alias
  const dbRecords = await privateServerDb.databases.list<DatabaseRecordValue>()
  const dbRecord = dbRecords.find(r => r.value?.services?.find(a => a.serviceId === serviceId && a.alias === alias))
  if (dbRecord?.value) {
    return dbRecord.value.dbId
  }
}

export async function createDb (serviceId: string, opts: DbSettings): Promise<GeneralDB> {
  if (!privateServerDb) {
    throw new Error('Cannot create new db: server db not available')
  }
  const netAccess = (opts.network?.access || 'public') as NetworkAccess
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
      if (!dbRecord.value) return false
      dbRecord.value.network = {access: netAccess}
      dbRecord.value.services = [{serviceId, alias: opts.alias, persist: true}]
      dbRecord.value.createdBy = {serviceId}
      dbRecord.value.createdAt = (new Date()).toISOString()
      return true
    })
  }
  return db
}

// Get a database by an application's alias ID. If a db does not exist at that alias, this function will create one.
export async function getOrCreateDbByAlias (serviceId: string, alias: string, settings: DbSettings): Promise<PrivateServerDB|GeneralDB> {
  const release = await lock(`get-or-create-db:${alias}`)
  try {
    const dbId = await resolveAlias(serviceId, alias)
    if (dbId) {
      const db = await loadDb(serviceId, dbId)
      if (!db) throw new Error(`Failed to load database: ${alias} (${dbId})`)
      return db
    } else {
      return createDb(serviceId, Object.assign({}, settings, {alias}))
    }
  } finally {
    release()
  }
}

// List databases attached to an application.
export function listServiceDbs (serviceId: string): Promise<ServiceDbConfig[]> {
  if (!privateServerDb) {
    throw new Error('Cannot list app db records: server db not available')
  }
  return privateServerDb.listServiceDbs(serviceId)
}

// Update the configuration of a database's attachment to an application.
export async function configureServiceDbAccess (serviceId: string, dbId: string, config: {persist?: boolean, presync?: boolean} = {}): Promise<DbRecord<DatabaseRecordValue>> {
  if (!privateServerDb) {
    throw new Error('Cannot configure app db access: server db not available')
  }
  if (!HYPER_KEY.test(dbId)) {
    const resolvedDbId = await resolveAlias(serviceId, dbId)
    if (!resolvedDbId) throw new Error(`Invalid database ID: ${dbId}`)
    dbId = resolvedDbId
  }
  return privateServerDb.configureServiceDbAccess(serviceId, dbId, config)
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


// Looks for any databases which are not in use and unloads them.
async function sweepInactiveDbs (): Promise<void> {
  const ts = Date.now()
  for (const db of getAllDbs()) {
    if (db.isEjectableFromMemory(ts)) {
      await db.teardown({unswarm: false})
    }
  }
}

export class GeneralDB extends BaseHyperbeeDB {
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
