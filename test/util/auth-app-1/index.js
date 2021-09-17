import express from 'express'
import { createRpcServer } from '@atek-cloud/node-rpc'
import adb, { defineTable } from '@atek-cloud/adb-api'
adb.api.$setEndpoint({port: 10000})

const TEST_RECORD_ID = 'auth-app.com/test'
const TEST_RECORD_DEFINITION = {
  "$schema":"http://json-schema.org/draft-07/schema#",
  "type":"object",
  "properties":{
    "id":{"type":"string"},
    "obj":{"type":"object","properties":{"bool":{"type":"boolean"}}},
    "createdAt":{"type":"string","format":"date-time"}
  },
  "required":["id","createdAt"]
}
const TEST_RECORD_TEMPLATES = {"table":{"title":"Test Records","description":"An example table."},"record":{"key":"{{/id}}","title":"Test record ID: {{/id}}"}};

const testTable = defineTable(TEST_RECORD_ID, {
  templates: TEST_RECORD_TEMPLATES,
  definition: TEST_RECORD_DEFINITION
})

const api = createRpcServer({
  async createDb () {
    const db = adb.db('mydb')
    await testTable(db).isReady
    return db.describe()
  },
  getDb () {
    const db = adb.db('mydb')
    return db.describe()
  },
  listDbs () {
    return adb.api.dbList()
  },
  listUserDbs (userKey) {
    return adb.api.adminListDbsByOwningUser(userKey)
  }
})

const SOCKETFILE = process.env.ATEK_ASSIGNED_SOCKET_FILE
const app = express()
app.use(express.json())
app.get('/', (req, res) => res.status(200).end('Hello!'))
app.post('/_api', (req, res) => api.handle(req, res, req.body))
app.listen(SOCKETFILE, e => {
  console.log(`auth-app-1 HTTP webserver running at`, SOCKETFILE)
})