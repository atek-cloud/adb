import { BaseHyperbeeDB, SetupOpts, DbRecord, Table, Auth } from './base.js'
import { loadDb, GeneralDB } from './index.js'
import { normalizeDbId } from '../lib/strings.js'
import { defined } from '../lib/functions.js'

import { Database, DATABASE, User, USER } from '@atek-cloud/adb-tables'
import { DbSettings } from '@atek-cloud/adb-api'

export interface ServiceDbConfig {
  dbId: string // The database identifier.
  displayName?: string // The user-friendly name of the database.
  writable?: boolean // Is the database writable?
  alias?: string // The alias ID of this database for this application.
  persist: boolean // Does this application want to keep the database in storage?
  presync: boolean // Does this application want the database to be fetched optimistically from the network?
}

export class PrivateServerDB extends BaseHyperbeeDB {
  databases: Table | undefined
  users: Table | undefined

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
    this.databases = this.table(DATABASE.ID, {
      revision: DATABASE.REVISION,
      templates: DATABASE.TEMPLATES,
      definition: DATABASE.DEFINITION
    })
    this.users = this.table(USER.ID, {
      revision: USER.REVISION,
      templates: USER.TEMPLATES,
      definition: USER.DEFINITION
    })
  }
  
  async onDatabaseCreated () {
    console.log('New private server database created, key:', this.dbId)
    await this.updateDesc({displayName: 'Server Registry'})
  }

  async getUser (userKey: string): Promise<DbRecord<User>|undefined> {
    return await this.users?.get(userKey)
  }

  async isUserAdmin (userKey: string): Promise<boolean> {
    if (userKey === 'system') return true
    const user = await this.getUser(userKey)
    return user?.value.role === 'admin'
  }

  // Get database config attached to an application.
  async getServiceDb (auth: Auth, dbId: string): Promise<ServiceDbConfig> {
    if (!this.databases) throw new Error('Cannot list app db record: this database is not setup')
    const dbRecord = await this.databases.get<Database>(dbId)
    if (dbRecord?.value) {
      const access = dbRecord.value.services?.find(a => a.serviceKey === auth.serviceKey)
      if (access) {
        return {
          dbId: dbRecord.value.dbId,
          displayName: dbRecord.value.cachedMeta?.displayName,
          writable: dbRecord.value.cachedMeta?.writable,
          alias: access.alias,
          persist: access.persist || false,
          presync: access.presync || false
        }
      }
    }
    throw new Error('No config for this database found')
  }

  // List databases attached to an application.
  async listServiceDbs (auth: Auth): Promise<ServiceDbConfig[]> {
    if (!this.databases) throw new Error('Cannot list app db record: this database is not setup')
    const databases: ServiceDbConfig[] = []
    const dbRecords = await this.databases.list<Database>()
    for (const dbRecord of dbRecords) {
      if (!dbRecord.value) continue
      const access = dbRecord.value.services?.find(a => a.serviceKey === auth.serviceKey)
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

  // List databases owned by a user
  async listUserDbs (auth: Auth, owningUserKey: string): Promise<ServiceDbConfig[]> {
    if (!this.databases) throw new Error('Cannot list app db record: this database is not setup')
    const isAdmin = await this.isUserAdmin(auth.userKey)
    if (auth.userKey !== owningUserKey && !isAdmin) {
      throw new Error('Not authorized')
    }
    const databases: ServiceDbConfig[] = []
    if (owningUserKey === 'system') {
      // add the server db
      databases.push({
        dbId: this.dbId || '',
        displayName: this.displayName,
        writable: this.writable,
        alias: '',
        persist: true,
        presync: false
      })
    }
    const dbRecords = await this.databases.list<Database>()
    for (const dbRecord of dbRecords) {
      if (!dbRecord.value) continue
      if (dbRecord.value.owningUserKey === owningUserKey) {
        databases.push({
          dbId: dbRecord.value.dbId,
          displayName: dbRecord.value.cachedMeta?.displayName,
          writable: dbRecord.value.cachedMeta?.writable,
          alias: '',
          persist: dbRecord.value.services?.reduce<boolean>((acc, s) => s.persist || acc, false) || false,
          presync: dbRecord.value.services?.reduce<boolean>((acc, s) => s.presync || acc, false) || false
        })
      }
    }
    return databases
  }

  // Update a database's record.
  async updateDbRecord (dbId: string, updateFn: (record: DbRecord<Database>) => boolean): Promise<DbRecord<Database>> {
    if (!this.databases) throw new Error('Cannot update db record: this database is not setup')
    dbId = normalizeDbId(dbId)
    const release = await this.databases.lock(dbId)
    try {
      let isNew = false
      let dbRecord = await this.databases.get<Database>(dbId)
      if (!dbRecord) {
        const db = await loadDb({serviceKey: 'system', userKey: 'system'}, dbId)
        if (!db) throw new Error(`Failed to load database: ${dbId}`)
        isNew = true
        dbRecord = {
          key: dbId,
          value: {
            dbId,
            owningUserKey: '',
            cachedMeta: {
              displayName: db.displayName,
              writable: db.writable
            },
            services: [],
            createdBy: {serviceKey: ''},
            createdAt: (new Date()).toISOString()
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
  updateDbRecordCachedMeta (db: PrivateServerDB|GeneralDB|BaseHyperbeeDB): Promise<DbRecord<Database>> {
    if (!db.dbId) throw new Error('Cannot update record cached meta: database not hydrated')
    return this.updateDbRecord(db.dbId, dbRecord => {
      if (!dbRecord.value) return false
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
  configureServiceDbAccess (auth: Auth, dbId: string, config: DbSettings = {}): Promise<DbRecord<Database>> {
    return this.updateDbRecord(dbId, dbRecord => {
      if (!dbRecord.value) return false
      if (!dbRecord.value.services) dbRecord.value.services = []
      let access = dbRecord.value.services.find(a => a.serviceKey === auth.serviceKey)
      if (!access) {
        access = {
          serviceKey: auth.serviceKey,
          persist: false,
          presync: false
        }
        dbRecord.value.services.push(access)
      }

      if (defined(config.alias)) access.alias = config.alias
      if (defined(config.persist)) access.persist = config.persist
      if (defined(config.presync)) access.presync = config.presync
      
      return true
    })
  }
}
