import createExpressApp, * as express from 'express'
import bodyParser from 'body-parser'
import * as db from './db/index.js'
import { DbDescription, Record, BlobMap, BlobDesc, Diff } from './gen/atek.cloud/adb-api.js'
import AdbApiServer from './gen/atek.cloud/adb-api.server.js'
import AdbCtrlApiServer from './gen/atek.cloud/adb-ctrl-api.server.js'

setup()

async function setup () {
  await db.setup()

  const adbCtrlApiServer = new AdbCtrlApiServer({
    getServerDatabaseId (): Promise<string> {
      return Promise.resolve(db.privateServerDb?.dbId || '')
    }
  })

  const adbApiServer = new AdbApiServer({
    // Get metadata and information about a database.
    describe (dbId: string): Promise<DbDescription> {
      throw "TODO"
    },

    // List records in a table.
    list (dbId: string, tableId: string): Promise<{records: Record[]}> {
      throw "TODO"
    },

    // Get a record in a table.
    get (dbId: string, tableId: string, key: string): Promise<Record> {
      throw "TODO"
    },

    // Add a record to a table.
    create (dbId: string, tableId: string, value: object, blobs?: BlobMap): Promise<Record> {
      throw "TODO"
    },

    // Write a record to a table.
    put (dbId: string, tableId: string, key: string, value: object): Promise<Record> {
      throw "TODO"
    },
    
    // Delete a record from a table.
    delete (dbId: string, tableId: string, key: string): Promise<void> {
      throw "TODO"
    },
    
    // Enumerate the differences between two versions of the database.
    diff (dbId: string, opts: {left: number, right?: number, tableIds?: string[]}): Promise<Diff[]> {
      throw "TODO"
    },

    // Get a blob of a record.
    getBlob (dbId: string, tableId: string, key: string, blobName: string): Promise<Buffer> {
      throw "TODO"
    },
    
    // Write a blob of a record.
    putBlob (dbId: string, tableId: string, key: string, blobName: string, blobValue: BlobDesc): Promise<void> {
      throw "TODO"
    },
    
    // Delete a blob of a record.
    delBlob (dbId: string, tableId: string, key: string, blobName: string): Promise<void> {
      throw "TODO"
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
}
