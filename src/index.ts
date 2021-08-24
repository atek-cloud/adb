import createExpressApp, * as express from 'express'
import bodyParser from 'body-parser'
import * as dbs from './db/index.js'
import { joinPath } from './lib/strings.js'
import { DbDescription, Record, BlobMap, BlobDesc, Diff, ListOpts, TableSettings, TableDescription } from './gen/atek.cloud/adb-api.js'
import AdbApiServer from './gen/atek.cloud/adb-api.server.js'
import { AdbProcessConfig, DbSettings, DbInfo } from './gen/atek.cloud/adb-ctrl-api.js'
import AdbCtrlApiServer from './gen/atek.cloud/adb-ctrl-api.server.js'

const adbCtrlApiServer = new AdbCtrlApiServer({
  // Initialize the ADB process
  init (config: AdbProcessConfig): Promise<void> {
    return dbs.setup(config)
  },

  // Get the ADB process configuration
  getConfig (): Promise<AdbProcessConfig> {
    return Promise.resolve({
      serverDbId: dbs.privateServerDb?.dbId || ''
    })
  },

  // Create a new database
  async createDb (settings: DbSettings): Promise<DbInfo> {
    const db = await dbs.createDb('system', settings)
    if (!db?.dbId) throw new Error('Failed to create database')
    return {dbId: db.dbId}
  },

  // Get or create a database according to an alias. Database aliases are local to each application.
  async getOrCreateDb (alias: string, settings: DbSettings): Promise<DbInfo> {
    const db = await dbs.getOrCreateDbByAlias('system', alias, settings)
    if (!db?.dbId) throw new Error('Failed to get or create database')
    return {dbId: db.dbId}
  },

  // Configure a database's settings
  async configureDb (dbId: string, settings: DbSettings): Promise<void> {
    await dbs.configureServiceDbAccess('system', dbId, settings)
  },

  // Configure a database's settings
  getDbConfig (dbId: string): Promise<DbSettings> {
    return dbs.getServiceDbConfig('system', dbId)
  },

  // Configure a database's settings
  listDbs (): Promise<DbSettings[]> {
    return dbs.listServiceDbs('service')
  }
})

const adbApiServer = new AdbApiServer({
  // Get metadata and information about a database.
  async describe (dbId: string): Promise<DbDescription> {
    const db = await dbs.loadDb('system', dbId)
    return {
      dbId,
      dbType: 'hyperbee',
      displayName: db.displayName,
      tables: Object.values(db.tables).map(t => ({
        tableId: t.schema.tableId,
        revision: t.schema.revision,
        templates: t.schema.templates,
        definition: t.schema.definition
      }))
    }
  },

  // Register a table's schema and metadata. 
  async table (dbId: string, tableId: string, desc: TableSettings): Promise<TableDescription> {
    const db = await dbs.loadDb('system', dbId)
    const schema = db.table(tableId, desc).schema
    return {
      tableId: schema.tableId,
      revision: schema.revision,
      templates: schema.templates,
      definition: schema.definition
    }
  },

  // List records in a table.
  async list (dbId: string, tableId: string, opts?: ListOpts): Promise<{records: Record[]}> {
    const db = await dbs.loadDb('system', dbId)
    const table = db.table(tableId)
    const records = await table.list<object>(opts)
    return {
      records: records.map(record => ({
        key: record.key,
        path: `/${joinPath(tableId, record.key)}`,
        url: joinPath(db.url, tableId, record.key),
        seq: record.seq,
        value: record.value
      }))
    }
  },

  // Get a record in a table.
  async get (dbId: string, tableId: string, key: string): Promise<Record> {
    const db = await dbs.loadDb('system', dbId)
    const table = db.table(tableId)
    const record = await table.get<object>(key)
    if (!record) throw new Error(`Not found: ${key}`)
    return {
      key: record.key,
      path: `/${joinPath(tableId, record.key)}`,
      url: joinPath(db.url, tableId, record.key),
      seq: record.seq,
      value: record.value
    }
  },

  // Add a record to a table.
  async create (dbId: string, tableId: string, value: object, blobs?: BlobMap): Promise<Record> {
    const db = await dbs.loadDb('system', dbId)
    const table = db.table(tableId)
    
    // TODO
    // table.schema.assertBlobMimeTypeValid(file.fieldname, file.mimetype)
    // table.schema.assertBlobSizeValid(file.fieldname, file.buffer.length)

    const key = table.schema.gen.record.key(value)
    if (value && typeof value === 'object' && !('createdAt' in value) && table.schema.hasCreatedAt) {
      Object.assign(value, {
        createdAt: (new Date()).toISOString()
      })
    }
    await table.put(key, value)

    if (blobs && Object.keys(blobs).length) {
      for (const blobName in blobs) {
        await table.putBlob(key, blobName, blobs[blobName].buf, {mimeType: blobs[blobName].mimeType})
      }
    }

    return {
      key: key,
      path: `/${joinPath(tableId, key)}`,
      url: joinPath(db.url, tableId, key),
      seq: undefined, // TODO needed?
      value
    }
  },

  // Write a record to a table.
  async put (dbId: string, tableId: string, key: string, value: object): Promise<Record> {
    const db = await dbs.loadDb('system', dbId)
    const table = db.table(tableId)
    await table.put(key, value)
    return {
      key: key,
      path: `/${joinPath(tableId, key)}`,
      url: joinPath(db.url, tableId, key),
      seq: undefined, // TODO needed?
      value
    }
  },
  
  // Delete a record from a table.
  async delete (dbId: string, tableId: string, key: string): Promise<void> {
    const db = await dbs.loadDb('system', dbId)
    const table = db.table(tableId)
    await table.del(key)
  },
  
  // Enumerate the differences between two versions of the database.
  async diff (dbId: string, opts: {left: number, right?: number, tableIds?: string[]}): Promise<Diff[]> {
    // TODO
    throw "TODO"
  },

  // Get a blob of a record.
  async getBlob (dbId: string, tableId: string, key: string, blobName: string): Promise<Buffer> {
    const db = await dbs.loadDb('system', dbId)
    const table = db.table(tableId)
    const {buf} = await table.getBlob(key, blobName, 'binary')
    return buf as Buffer
  },
  
  // Write a blob of a record.
  async putBlob (dbId: string, tableId: string, key: string, blobName: string, blobValue: BlobDesc): Promise<void> {
    const db = await dbs.loadDb('system', dbId)
    const table = db.table(tableId)
    await table.putBlob(key, blobName, blobValue.buf, {mimeType: blobValue.mimeType})
  },
  
  // Delete a blob of a record.
  async delBlob (dbId: string, tableId: string, key: string, blobName: string): Promise<void> {
    const db = await dbs.loadDb('system', dbId)
    const table = db.table(tableId)
    await table.delBlob(key, blobName)
  }
})

const PORT = Number(process.env.ATEK_ASSIGNED_PORT)
const app = createExpressApp()
app.use(bodyParser.json())
app.post('/_api/adb-ctrl', (req, res) => adbCtrlApiServer.handle(req, res, req.body))
app.post('/_api/adb', (req, res) => adbApiServer.handle(req, res, req.body))
app.get('/', (req: express.Request, res: express.Response) => {
  res.status(200).end('ADB server active')
})
app.listen(PORT, () => {
  console.log(`ADB server running at: http://localhost:${PORT}/`)
})