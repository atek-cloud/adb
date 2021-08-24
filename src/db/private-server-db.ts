import { BaseHyperbeeDB, SetupOpts, DbRecord, Table } from './base.js'
import { loadDb, GeneralDB } from './index.js'
import { normalizeDbId } from '../lib/strings.js'
import { defined } from '../lib/functions.js'

import DatabaseRecordValue, * as DatabaseRecords from '../gen/atek.cloud/database.js'
import { DbSettings } from '../gen/atek.cloud/adb-ctrl-api.js'

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
    this.databases = this.table(DatabaseRecords.ID, {
      revision: DatabaseRecords.REVISION,
      templates: DatabaseRecords.TEMPLATES,
      definition: DatabaseRecords.JSON_SCHEMA
    })
  }
  
  async onDatabaseCreated () {
    console.log('New private server database created, key:', this.dbId)
    await this.updateDesc({displayName: 'Server Registry'})
  }

  // Get database config attached to an application.
  async getServiceDb (serviceId: string, dbId: string): Promise<ServiceDbConfig> {
    if (!this.databases) throw new Error('Cannot list app db record: this database is not setup')
    const dbRecord = await this.databases.get<DatabaseRecordValue>(dbId)
    if (dbRecord?.value) {
      const access = dbRecord.value.services?.find(a => a.serviceId === serviceId)
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
  async listServiceDbs (serviceId: string): Promise<ServiceDbConfig[]> {
    if (!this.databases) throw new Error('Cannot list app db record: this database is not setup')
    const databases: ServiceDbConfig[] = []
    const dbRecords = await this.databases.list<DatabaseRecordValue>()
    for (const dbRecord of dbRecords) {
      if (!dbRecord.value) continue
      const access = dbRecord.value.services?.find(a => a.serviceId === serviceId)
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
  async updateDbRecord (dbId: string, updateFn: (record: DbRecord<DatabaseRecordValue>) => boolean): Promise<DbRecord<DatabaseRecordValue>> {
    if (!this.databases) throw new Error('Cannot update db record: this database is not setup')
    dbId = normalizeDbId(dbId)
    const release = await this.databases.lock(dbId)
    try {
      let isNew = false
      let dbRecord = await this.databases.get<DatabaseRecordValue>(dbId)
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
            services: [],
            createdBy: {serviceId: '', accountId: ''},
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
  updateDbRecordCachedMeta (db: PrivateServerDB|GeneralDB|BaseHyperbeeDB): Promise<DbRecord<DatabaseRecordValue>> {
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
  configureServiceDbAccess (serviceId: string, dbId: string, config: DbSettings = {}): Promise<DbRecord<DatabaseRecordValue>> {
    return this.updateDbRecord(dbId, dbRecord => {
      if (!dbRecord.value) return false
      if (!dbRecord.value.services) dbRecord.value.services = []
      let access = dbRecord.value.services.find(a => a.serviceId === serviceId)
      if (!access) {
        access = {
          serviceId,
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
