import { BaseHyperbeeDB, SetupOpts, DbRecord } from './base.js'
import { loadDb, getDb, GeneralDB } from './index.js'
import { Auth } from './permissions.js'
import { normalizeDbId } from '../lib/strings.js'
import { defined } from '../lib/functions.js'
import { DbConfig, DbAdminConfig, DbInfo } from '@atek-cloud/adb-api'
import { DB_PATH, USER_PATH, SERVICE_PATH, dbValidator, Database, User, Service, DatabaseAccess } from './schemas.js'

const MY_SKEY = process.env.ATEK_ASSIGNED_SERVICE_KEY

export class PrivateServerDB extends BaseHyperbeeDB {
  constructor ({key}: {key: string|undefined}) {
    super({
      key,
      access: DatabaseAccess.private
    })
  }

  isEjectableFromMemory (ts: number) {
    return false // never eject the private server db from memory
  }

  async setup (opts: SetupOpts) {
    await super.setup(opts)
  }
  
  async onDatabaseCreated () {
    console.log('New private server database created, key:', this.dbId)
  }

  getUser (userKey: string): Promise<DbRecord<User>|undefined> {
    return this.get<User>([...USER_PATH, userKey])
  }

  async isUserAdmin (userKey: string): Promise<boolean> {
    if (userKey === 'system') return true
    const user = await this.getUser(userKey)
    return user?.value?.role === 'admin'
  }

  async getServiceOwnerKey (serviceKey: string): Promise<string|undefined> {
    if (serviceKey === process.env.ATEK_ASSIGNED_SERVICE_KEY) return 'system'
    const serviceRecord = await this.get<Service>([...SERVICE_PATH, serviceKey])
    return serviceRecord?.value?.owningUserKey
  }

  // Get database config.
  async getDbConfig (auth: Auth, dbId: string): Promise<DbConfig> {
    await auth.assertCanReadDatabase(dbId)
    const dbRecord = await this.get<Database>([...DB_PATH, dbId])
    if (dbRecord?.value) {
      return {
        alias: dbRecord.value.alias,
        access: dbRecord.value.access
      }
    }
    throw new Error('No config for this database found')
  }

  // Get database info.
  async getDbInfo (auth: Auth, dbId: string): Promise<DbInfo> {
    await auth.assertCanReadDatabase(dbId)
    const dbRecord = await this.get<Database>([...DB_PATH, dbId])
    if (dbRecord?.value) {
      return {
        dbId: dbRecord.value.dbId,
        writable: dbRecord.value.cachedMeta?.writable,
        isServerDb: dbRecord.value.dbId === this.dbId,
        owner: dbRecord.value.owner,
        alias: dbRecord.value.alias,
        access: dbRecord.value.access,
        createdAt: dbRecord.value.createdAt
      }
    }
    if (dbId === this.dbId) {
      return {
        dbId,
        writable: true,
        isServerDb: true,
        owner: {userKey: 'system', serviceKey: 'system'},
        access: 'private'
      }
    }
    return {
      dbId,
      writable: false,
      isServerDb: false
    }
  }

  // List databases attached to an application.
  async listServiceDbs (auth: Auth): Promise<DbInfo[]> {
    const databases: DbInfo[] = []
    const dbRecords = await this.list<Database>(DB_PATH)
    for (const dbRecord of dbRecords) {
      if (!dbRecord.value) continue
      if (dbRecord.value.owner?.serviceKey !== auth.serviceKey) continue
      databases.push({
        dbId: dbRecord.value.dbId,
        writable: dbRecord.value.cachedMeta?.writable,
        isServerDb: dbRecord.value.dbId === this.dbId,
        owner: dbRecord.value.owner,
        alias: dbRecord.value.alias,
        access: dbRecord.value.access,
        createdAt: dbRecord.value.createdAt
      })
    }
    return databases
  }

  // List databases owned by a user
  async adminListDbsByOwningUser (auth: Auth, owningUserKey: string): Promise<DbInfo[]> {
    const databases: DbInfo[] = []
    if (owningUserKey === 'system') {
      // add the server db
      databases.push({
        dbId: this.dbId || '',
        writable: this.writable,
        isServerDb: true,
        owner: {
          userKey: 'system',
          serviceKey: 'system'
        },
        access: 'private'
      })
    }
    const dbRecords = await this.list<Database>(DB_PATH)
    for (const dbRecord of dbRecords) {
      if (dbRecord.value?.owner?.userKey === owningUserKey) {
        databases.push({
          dbId: dbRecord.value.dbId,
          writable: dbRecord.value.cachedMeta?.writable,
          isServerDb: false,
          owner: dbRecord.value.owner,
          alias: dbRecord.value.alias,
          access: dbRecord.value.access,
          createdAt: dbRecord.value.createdAt
        })
      }
    }
    return databases
  }

  // (admin access) Delete a DB
  async adminDeleteDb (auth: Auth, dbId: string) {
    const release = await this.lockPath(DB_PATH)
    try {
      const dbRecord = await this.get<Database>([...DB_PATH, dbId])
      if (!dbRecord?.value) throw new Error('DB not found')
      await auth.assertCanWriteDatabaseRecord(dbRecord.value, undefined)
      await this.del([...DB_PATH, dbId])

      const db = getDb(dbId)
      if (db) {
        db.onConfigUpdated(dbRecord.value)
      }
    } finally {
      release()
    }
  }

  // Update a database's record.
  async updateDbRecord (auth: Auth, dbId: string, updateFn: (record: DbRecord<Database>) => boolean): Promise<DbRecord<Database>> {
    dbId = normalizeDbId(dbId)
    const release = await this.lockPath(DB_PATH)
    try {
      let isNew = false
      let dbRecord = await this.get<Database>([...DB_PATH, dbId])
      const oldValue = dbRecord?.value ? JSON.parse(JSON.stringify(dbRecord.value)) : undefined
      if (!dbRecord?.value) {
        const db = await loadDb(auth, dbId)
        if (!db) throw new Error(`Failed to load database: ${dbId}`)
        isNew = true
        dbRecord = {
          key: dbId,
          value: {
            dbId,
            cachedMeta: {
              writable: db.writable
            },
            createdAt: (new Date()).toISOString()
          }
        }
      }
      const wasChanged = updateFn(dbRecord)
      if (wasChanged !== false || isNew) {
        await auth.assertCanWriteDatabaseRecord(oldValue, dbRecord.value)
        if (!dbValidator.validate(dbRecord.value)) {
          console.error('Warning: a database record failed validation on write')
          console.error('  Error:', dbValidator.errors()?.[0].propertyName || dbValidator.errors()?.[0].instancePath, dbValidator.errors()?.[0].message)
          console.error('  Key:', dbRecord.key)
          console.error('  Value:', dbRecord.value)
        }
        await this.put([...DB_PATH, dbRecord.key], dbRecord.value)

        const db = getDb(dbId)
        if (db) {
          db.onConfigUpdated(dbRecord.value)
        }
      }
      return dbRecord
    } finally {
      release()
    }
  }

  // Update a database record's cached metadata.
  updateDbRecordCachedMeta (db: PrivateServerDB|GeneralDB|BaseHyperbeeDB): Promise<DbRecord<Database>> {
    if (!db.dbId) throw new Error('Cannot update record cached meta: database not hydrated')
    const auth = new Auth('system', MY_SKEY || 'system')
    return this.updateDbRecord(auth, db.dbId, dbRecord => {
      if (!dbRecord.value) return false
      if (!dbRecord.value.cachedMeta) {
        dbRecord.value.cachedMeta = {}
      }
      if (dbRecord.value.cachedMeta.writable === db.writable) {
        return false
      }
      dbRecord.value.cachedMeta.writable = db.writable
      return true
    })
  }

  // Update the configuration of a database's attachment to an application.
  configureDb (auth: Auth, dbId: string, config: DbAdminConfig = {}): Promise<DbRecord<Database>> {
    return this.updateDbRecord(auth, dbId, dbRecord => {
      if (!dbRecord.value) return false
      let hasChanged = false
      const apply = (key: (keyof DbAdminConfig & keyof Database)) => {
        if (defined(config[key]) && config[key] !== dbRecord.value[key]) {
          hasChanged = true
          // @ts-ignore can't get TS happy with dbRecord.value here, not sure why -prf
          dbRecord.value[key] = config[key]
        }
      }
      apply('alias')
      apply('access')
      if (config.owner) {
        apply('owner')
      }
      return hasChanged
    })
  }
}
