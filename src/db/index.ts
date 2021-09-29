import * as hyperspace from './hyperspace.js'
import { BaseHyperbeeDB, SetupOpts, DbRecord } from './base.js'
import { PrivateServerDB } from './private-server-db.js'
import { Auth } from './permissions.js'
import { HYPER_KEY, normalizeDbId } from '../lib/strings.js'
import { CaseInsensitiveMap } from '../lib/map.js'
import lock from '../lib/lock.js'
import { defined } from '../lib/functions.js'

import { AdbProcessConfig, DbConfig, DbAdminConfig, DbInfo } from '@atek-cloud/adb-api'
import { DB_PATH, Database, DatabaseAccess } from './schemas.js'

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
  if (!privateServerDb) {
    throw new Error('Server db not available')
  }
  if (!dbId) throw new Error(`Must provide a dbId to loadDb()`)
  dbId = normalizeDbId(dbId)
  await auth.assertCanReadDatabase(dbId)
  const release = await lock(`load-db:${dbId}`)
  try {
    if (dbId === privateServerDb.dbId) return privateServerDb
    const dbRecord: DbRecord<Database> | undefined = (await privateServerDb.get<Database>([...DB_PATH, dbId]))
    let db = getDb(dbId)
    if (!db) {
      db = new GeneralDB({
        key: dbId,
        access: (dbRecord?.value?.access || DatabaseAccess.public) as DatabaseAccess
      })
      await db.setup({create: false})
      dbs.set(dbId, db)
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
  if (!privateServerDb) {
    throw new Error('Cannot resolve alias: server db not available')
  }
  if (HYPER_KEY.test(alias)) return alias
  const dbRecords = await privateServerDb.list<Database>(DB_PATH)
  const dbRecord = dbRecords.find(r => r.value?.alias === alias && r.value?.owner?.serviceKey === auth.serviceKey)
  if (dbRecord?.value) {
    return dbRecord.value.dbId
  }
}

export async function createDb (auth: Auth, config: DbConfig): Promise<GeneralDB> {
  if (!privateServerDb) {
    throw new Error('Cannot create new db: server db not available')
  }
  const access = (config.access || 'public') as DatabaseAccess
  const db = new GeneralDB({access})
  await db.setup({create: true})
  if (db.dbId) {
    dbs.set(db.dbId, db)
    await privateServerDb.updateDbRecord(auth, db.dbId, dbRecord => {
      if (!dbRecord.value) return false
      dbRecord.value.owner = {userKey: auth.userKey, serviceKey: auth.serviceKey}
      dbRecord.value.access = access
      dbRecord.value.alias = config.alias
      dbRecord.value.createdAt = (new Date()).toISOString()
      return true
    })
  }
  return db
}

// Get a database by an application's alias ID. If a db does not exist at that alias, this function will create one.
export async function getOrCreateDbByAlias (auth: Auth, alias: string, config: DbConfig): Promise<PrivateServerDB|GeneralDB> {
  const release = await lock(`get-or-create-db:${alias}`)
  try {
    const dbId = await resolveAlias(auth, alias)
    if (dbId) {
      const db = await loadDb(auth, dbId)
      if (!db) throw new Error(`Failed to load database: ${alias} (${dbId})`)
      return db
    } else {
      return createDb(auth, Object.assign({}, config, {alias}))
    }
  } finally {
    release()
  }
}

// List databases attached to an application.
export function listServiceDbs (auth: Auth): Promise<DbInfo[]> {
  if (!privateServerDb) {
    throw new Error('Cannot list app db records: server db not available')
  }
  return privateServerDb.listServiceDbs(auth)
}

export function adminListDbsByOwningUser (auth: Auth, userKey: string): Promise<DbInfo[]> {
  if (!privateServerDb) {
    throw new Error('Cannot list app db records: server db not available')
  }
  return privateServerDb.adminListDbsByOwningUser(auth, userKey)
}

export async function adminCreateDb (auth: Auth, config: DbAdminConfig): Promise<GeneralDB> {
  if (!privateServerDb) {
    throw new Error('Cannot create db: server db not available')
  }

  const owner = config.owner
  const alias = config.alias
  const access = (config.access || 'public') as DatabaseAccess

  // run this assert early to avoid creating more DBs than needed
  await auth.assertCanWriteDatabaseRecord(undefined, {dbId: '', owner, alias, access, createdAt: ''})

  const db = new GeneralDB({access})
  await db.setup({create: true})
  if (db.dbId) {
    dbs.set(db.dbId, db)
    await privateServerDb.updateDbRecord(auth, db.dbId, dbRecord => {
      if (!dbRecord.value) return false
      dbRecord.value.owner = config.owner
      dbRecord.value.alias = config.alias
      dbRecord.value.access = access
      dbRecord.value.createdAt = (new Date()).toISOString()
      return true
    })
  }
  return db
}

export function adminDeleteDb (auth: Auth, dbId: string) {
  if (!privateServerDb) {
    throw new Error('Server db not available')
  }
  return privateServerDb.adminDeleteDb(auth, dbId)
}

// Update the configuration of a database's attachment to an application.
export async function configureDb (auth: Auth, dbId: string, config: DbConfig): Promise<DbRecord<Database>> {
  if (!privateServerDb) {
    throw new Error('Cannot configure app db access: server db not available')
  }
  if (!HYPER_KEY.test(dbId)) {
    const resolvedDbId = await resolveAlias(auth, dbId)
    if (!resolvedDbId) throw new Error(`Invalid database ID: ${dbId}`)
    dbId = resolvedDbId
  }
  // TODO: react to an 'access' change
  return privateServerDb.configureDb(auth, dbId, config)
}

export async function getDbConfig (auth: Auth, dbId: string): Promise<DbConfig> {
  if (!privateServerDb) {
    throw new Error('Cannot get app db access: server db not available')
  }
  if (!HYPER_KEY.test(dbId)) {
    const resolvedDbId = await resolveAlias(auth, dbId)
    if (!resolvedDbId) throw new Error(`Invalid database ID: ${dbId}`)
    dbId = resolvedDbId
  }
  return privateServerDb.getDbConfig(auth, dbId)
}

export async function getDbInfo (auth: Auth, dbId: string): Promise<DbInfo> {
  if (!privateServerDb) {
    throw new Error('Cannot get app db access: server db not available')
  }
  if (!HYPER_KEY.test(dbId)) {
    const resolvedDbId = await resolveAlias(auth, dbId)
    if (!resolvedDbId) throw new Error(`Invalid database ID: ${dbId}`)
    dbId = resolvedDbId
  }
  return privateServerDb.getDbInfo(auth, dbId)
}

// Waits for all databases to finish their sync events.
// NOTE: this method should only be used for tests
export async function whenAllSynced (): Promise<void> {
  for (let db of getAllDbs()) {
    await db.whenSynced()
  }
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

export class GeneralDB extends BaseHyperbeeDB {
  async setup (opts: SetupOpts) {
    await super.setup(opts)
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
