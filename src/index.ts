import createExpressApp, * as express from 'express'
import bodyParser from 'body-parser'
import { dirname, join } from 'path'
import { fileURLToPath } from 'url'
import { Auth } from './db/permissions.js'
import * as dbs from './db/index.js'
import { joinPath } from './lib/strings.js'
import { createRpcServer } from '@atek-cloud/node-rpc'
import { createServer as createAdbServer, AdbProcessConfig, DbInfo, DbConfig, DbAdminConfig, Record, ListOpts } from '@atek-cloud/adb-api'
import services from '@atek-cloud/services-api'

const __dirname = join(dirname(fileURLToPath(import.meta.url)), '..')
const MY_SKEY = process.env.ATEK_ASSIGNED_SERVICE_KEY

function getAuth (req: express.Request): Auth {
  const userKey = typeof req.headers['atek-auth-user'] === 'string' ? req.headers['atek-auth-user'] : undefined
  const serviceKey = typeof req.headers['atek-auth-service'] === 'string' ? req.headers['atek-auth-service'] : undefined
  if (!userKey || !serviceKey) throw new Error('Not authorized')
  return new Auth(userKey, serviceKey)
}

const adbApiServer = createAdbServer({
  // Initialize the ADB process
  init (config: AdbProcessConfig): Promise<void> {
    if (getAuth(this.req).userKey !== 'system') throw new Error('Not authorized')
    return dbs.setup(config)
  },

  // Get the ADB process configuration
  getConfig (): Promise<AdbProcessConfig> {
    if (getAuth(this.req).userKey !== 'system') throw new Error('Not authorized')
    return Promise.resolve({
      serverDbId: dbs.privateServerDb?.dbId || ''
    })
  },

  // List all databases owned by a user
  async adminListDbsByOwningUser (owningUserKey: string): Promise<DbInfo[]> {
    const auth = getAuth(this.req)
    owningUserKey = owningUserKey || auth.userKey
    await auth.assertCanEnumerateDatabasesOwnedByUser(owningUserKey)

    return dbs.adminListDbsByOwningUser(auth, owningUserKey)
  },

  // Create a new database under a specific service
  async adminCreateDb (config: DbAdminConfig): Promise<DbInfo> {
    const auth = getAuth(this.req)

    config.owner = config.owner || {}
    if (!config.owner.userKey) config.owner.userKey = auth.userKey
    if (!config.owner.serviceKey) config.owner.serviceKey = auth.serviceKey

    const db = await dbs.adminCreateDb(auth, config)
    if (!db?.dbId) throw new Error('Failed to create database')
    return {dbId: db.dbId, writable: db.writable} // TODO
  },

  // Edit a service's config for a db
  async adminEditDbConfig (dbId: string, config: DbAdminConfig): Promise<void> {
    const auth = getAuth(this.req)
    await dbs.configureDb(auth, dbId, config)
  },
  
  /**
   * @desc Delete a db
   */
  async adminDeleteDb (dbId: string): Promise<void> {
    const auth = getAuth(this.req)
    await dbs.adminDeleteDb(auth, dbId)
  },

  // Create a new database
  async dbCreate (config: DbConfig): Promise<DbInfo> {
    const db = await dbs.createDb(getAuth(this.req), config)
    if (!db?.dbId) throw new Error('Failed to create database')
    return {dbId: db.dbId, writable: db.writable} // TODO
  },

  // Get or create a database according to an alias. Database aliases are local to each application.
  async dbGetOrCreate (alias: string, config: DbConfig): Promise<DbInfo> {
    const db = await dbs.getOrCreateDbByAlias(getAuth(this.req), alias, config)
    if (!db?.dbId) throw new Error('Failed to get or create database')
    return {dbId: db.dbId, writable: db.writable}
  },

  // Configure a database's settings
  async dbConfigure (dbId: string, config: DbConfig): Promise<void> {
    const auth = getAuth(this.req)
    await dbs.configureDb(auth, dbId, config)
  },

  // Get a database's settings
  dbGetConfig (dbId: string): Promise<DbConfig> {
    const auth = getAuth(this.req)
    return dbs.getDbConfig(auth, dbId)
  },

  // List all databases configured to the calling service
  dbList (): Promise<DbInfo[]> {
    return dbs.listServiceDbs(getAuth(this.req))
  },
  
  // Get metadata and information about a database.
  dbDescribe (dbId: string): Promise<DbInfo> {
    const auth = getAuth(this.req)
    return dbs.getDbInfo(auth, dbId)
  },

  // List records in a path.
  async recordList (dbId: string, path: string|string[], opts?: ListOpts): Promise<{records: Record[]}> {
    path = Array.isArray(path) ? path : path.split('/').filter(Boolean)

    const auth = getAuth(this.req)
    const db = await dbs.loadDb(auth, dbId)
    const records = await db.list<object>(path, opts)
    return {
      records: records.map(record => {
        const keyParts = record.key.split('\x00').filter(Boolean)
        return {
          key: keyParts.join('/'),
          path: `/${joinPath(...path, ...keyParts)}`,
          url: joinPath(db.url, ...path, ...keyParts),
          seq: record.seq,
          value: record.value || {}
        }
      })
    }
  },

  // Get a record in a table.
  async recordGet (dbId: string, path: string|string[]): Promise<Record> {
    path = Array.isArray(path) ? path : path.split('/').filter(Boolean)

    const auth = getAuth(this.req)
    const db = await dbs.loadDb(auth, dbId)
    const record = await db.get<object>(path)
    if (!record) throw new Error(`Not found: ${path}`)
    const rpath = `/${joinPath(...path)}`
    return {
      key: record.key,
      path: rpath,
      url: joinPath(db.url, rpath),
      seq: record.seq,
      value: record.value || {}
    }
  },

  // Write a record to a table.
  async recordPut (dbId: string, path: string|string[], value: object): Promise<Record> {
    path = Array.isArray(path) ? path : path.split('/').filter(Boolean)

    const auth = getAuth(this.req)
    const db = await dbs.loadDb(auth, dbId)
    await db.put(path, value)
    return {
      key: path[path.length - 1],
      path: `/${joinPath(...path)}`,
      url: joinPath(db.url, ...path),
      seq: undefined, // TODO needed?
      value
    }
  },
  
  // Delete a record from a table.
  async recordDelete (dbId: string, path: string|string[]): Promise<void> {
    const auth = getAuth(this.req)
    const db = await dbs.loadDb(auth, dbId)
    await db.del(path)
  },
  
  // Enumerate the differences between two versions of the database.
  // async recordDiff (dbId: string, opts: {left: number, right?: number, tableIds?: string[]}): Promise<Diff[]> {
  //   // TODO
  //   throw "TODO"
  // }
})


const frontendApiServer = createRpcServer({
  getAppInfo () {
    return {serviceKey: MY_SKEY}
  },
  listServices () {
    const auth = getAuth(this.req)
    if (auth.serviceKey !== MY_SKEY) throw new Error('Not authorized')
    return services.list()
  },
  getService (id: string) {
    const auth = getAuth(this.req)
    if (auth.serviceKey !== MY_SKEY) throw new Error('Not authorized')
    return services.get(id)
  }
})

const SOCKETFILE = process.env.ATEK_ASSIGNED_SOCKET_FILE
const app = createExpressApp()
app.use(bodyParser.json())
app.post('/_api/adb', (req, res) => adbApiServer.handle(req, res, req.body))
app.post('/_api/frontend', (req, res) => frontendApiServer.handle(req, res, req.body))
app.use(express.static(join(__dirname, 'frontend')))
app.use((req, res) => res.sendFile(join(__dirname, 'frontend/index.html')))
app.listen(SOCKETFILE, () => {
  console.log(`ADB server running at: ${SOCKETFILE}`)
})