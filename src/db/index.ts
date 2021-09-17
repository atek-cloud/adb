import * as hyperspace from './hyperspace.js'
import { BaseHyperbeeDB, SetupOpts, DbRecord, Auth } from './base.js'
import { PrivateServerDB, ServiceDbConfig } from './private-server-db.js'
import { HYPER_KEY, normalizeDbId } from '../lib/strings.js'
import { CaseInsensitiveMap } from '../lib/map.js'
import lock from '../lib/lock.js'
import { defined } from '../lib/functions.js'

import { AdbProcessConfig, DbSettings } from '@atek-cloud/adb-api'
import { Database, DatabaseNetworkAccess } from '@atek-cloud/adb-tables'

const SWEEP_INACTIVE_DBS_INTERVAL = 10e3

// exported api
// =

export let privateServerDb: PrivateServerDB | undefined = undefined
export const dbs = new CaseInsensitiveMap<PrivateServerDB|GeneralDB>()

// Initialize the database system. Must be called during setup.
export async function setup(settings: AdbProcessConfig): Promise<void> {
  if (privateServerDb) {
    throw new Error('ADB has already been initialized')
  }

  await hyperspace.setup()
  
  privateServerDb = new PrivateServerDB({key: settings.serverDbId})
  await privateServerDb.setup({create: !settings.serverDbId})
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
// The access settings by `auth.serviceKey` is tracked automatically in the database.
export async function loadDb (auth: Auth, dbId: string): Promise<PrivateServerDB|GeneralDB> {
  if (!privateServerDb || !privateServerDb.databases) {
    throw new Error('Cannot resolve alias: server db not available')
  }
  if (!auth.serviceKey) throw new Error(`Not authorized`)
  if (!dbId) throw new Error(`Must provide a dbId to loadDb()`)
  dbId = normalizeDbId(dbId)
  const release = await lock(`load-db:${dbId}`)
  try {
    if (dbId === privateServerDb.dbId) return privateServerDb
    const dbRecord = (await privateServerDb.databases.get<Database>(dbId))
    let db = getDb(dbId)
    if (!db) {
      db = new GeneralDB({
        key: dbId,
        network: dbRecord?.value?.network || {access: 'public'}
      })
      await db.setup({create: false})
      dbs.set(dbId, db)
    }
    if (auth.serviceKey !== 'system' && !dbRecord?.value?.services?.find(a => a.serviceKey === auth.serviceKey)) {
      await privateServerDb.configureServiceDbAccess(auth, dbId, {persist: false})
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
export async function resolveAlias (auth: Auth, alias: string): Promise<string|undefined> {
  if (!privateServerDb || !privateServerDb.databases) {
    throw new Error('Cannot resolve alias: server db not available')
  }
  if (HYPER_KEY.test(alias)) return alias
  const dbRecords = await privateServerDb.databases.list<Database>()
  const dbRecord = dbRecords.find(r => r.value?.services?.find(a => a.serviceKey === auth.serviceKey && a.alias === alias))
  if (dbRecord?.value) {
    return dbRecord.value.dbId
  }
}

export async function createDb (auth: Auth, opts: DbSettings): Promise<GeneralDB> {
  if (!privateServerDb) {
    throw new Error('Cannot create new db: server db not available')
  }
  const netAccess = (opts.network?.access || 'public') as DatabaseNetworkAccess
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
      dbRecord.value.owningUserKey = auth.userKey
      dbRecord.value.network = {access: netAccess}
      dbRecord.value.services = [{
        serviceKey: auth.serviceKey,
        alias: opts.alias,
        persist: defined(opts.persist) ? opts.persist : true,
        presync: defined(opts.presync) ? opts.presync : false
      }]
      dbRecord.value.createdBy = {serviceKey: auth.serviceKey}
      dbRecord.value.createdAt = (new Date()).toISOString()
      return true
    })
  }
  return db
}

// Get a database by an application's alias ID. If a db does not exist at that alias, this function will create one.
export async function getOrCreateDbByAlias (auth: Auth, alias: string, settings: DbSettings): Promise<PrivateServerDB|GeneralDB> {
  const release = await lock(`get-or-create-db:${alias}`)
  try {
    const dbId = await resolveAlias(auth, alias)
    if (dbId) {
      const db = await loadDb(auth, dbId)
      if (!db) throw new Error(`Failed to load database: ${alias} (${dbId})`)
      return db
    } else {
      return createDb(auth, Object.assign({}, settings, {alias}))
    }
  } finally {
    release()
  }
}

// List databases attached to an application.
export function listServiceDbs (auth: Auth): Promise<ServiceDbConfig[]> {
  if (!privateServerDb) {
    throw new Error('Cannot list app db records: server db not available')
  }
  return privateServerDb.listServiceDbs(auth)
}

export function listUserDbs (auth: Auth, userKey: string): Promise<ServiceDbConfig[]> {
  if (!privateServerDb) {
    throw new Error('Cannot list app db records: server db not available')
  }
  return privateServerDb.listUserDbs(auth, userKey)
}

// Update the configuration of a database's attachment to an application.
export async function configureServiceDbAccess (auth: Auth, dbId: string, settings: DbSettings): Promise<DbRecord<Database>> {
  if (!privateServerDb) {
    throw new Error('Cannot configure app db access: server db not available')
  }
  if (!HYPER_KEY.test(dbId)) {
    const resolvedDbId = await resolveAlias(auth, dbId)
    if (!resolvedDbId) throw new Error(`Invalid database ID: ${dbId}`)
    dbId = resolvedDbId
  }
  const res = await privateServerDb.configureServiceDbAccess(auth, dbId, settings)

  if (defined(settings.displayName)) {
    const db = await loadDb(auth, dbId)
    await db.updateDesc({displayName: settings.displayName})
  }

  return res
}

export async function getServiceDbConfig (auth: Auth, dbId: string): Promise<ServiceDbConfig> {
  if (!privateServerDb) {
    throw new Error('Cannot get app db access: server db not available')
  }
  if (!HYPER_KEY.test(dbId)) {
    const resolvedDbId = await resolveAlias(auth, dbId)
    if (!resolvedDbId) throw new Error(`Invalid database ID: ${dbId}`)
    dbId = resolvedDbId
  }
  return privateServerDb.getServiceDb(auth, dbId)
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
