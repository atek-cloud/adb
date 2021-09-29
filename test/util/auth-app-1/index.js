import express from 'express'
import { createRpcServer } from '@atek-cloud/node-rpc'
import adb from '@atek-cloud/adb-api'
adb.api.$setEndpoint({port: 10000})

const api = createRpcServer({
  createDb () {
    const db = adb.db('mydb')
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