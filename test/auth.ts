import test from 'ava'
import * as atek from '@atek-cloud/atek'
import * as path from 'path'
import { fileURLToPath } from 'url'
import adb, {createClient as createAdbClient} from '@atek-cloud/adb-api'
adb.api.$setEndpoint({port: 10000})

const HERE_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), '..')
const AUTH_APP1_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), 'util', 'auth-app-1')
const AUTH_APP2_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), 'util', 'auth-app-2')

let inst: any
let activeCfg: any
test.after(async () => {
  await inst.close()
})

test.serial('Load test atek instance', async t => {
  const cfg = new atek.test.Config({
    coreServices: [
      {
        sourceUrl: 'https://github.com/atek-cloud/hyper-daemon',
        config: {SIMULATE_HYPERSPACE: '1'}
      },
      {sourceUrl: `file://${HERE_PATH}`}
    ]
  })
  inst = await atek.test.startAtek(cfg)
  adb.api.$setAuthHeader(`Bearer ${inst.authToken}`)

  activeCfg = await inst.api('atek.cloud/inspect-api').call('getConfig')
  t.truthy(activeCfg.serverDbId, 'Server DB ID was created')
})

test.serial('Access a database from a user application', async t => {
  const usersapi = inst.api('atek.cloud/users-api')
  const sessapi = inst.api('atek.cloud/user-sessions-api', {noAuth: true})
  const srvapi = inst.api('atek.cloud/services-api', {noAuth: true})
  const authApp1Api = inst.api('auth-app-one.com/api', {noAuth: true})
  const authApp2Api = inst.api('auth-app-two.com/api', {noAuth: true})

  const user = await usersapi.call('create', [{username: 'bob', password: 'hunter2'}])
  await sessapi.call('login', [{username: 'bob', password: 'hunter2'}])
  sessapi.copyCookiesTo(srvapi.cookieJar)
  sessapi.copyCookiesTo(authApp1Api.cookieJar)
  sessapi.copyCookiesTo(authApp2Api.cookieJar)

  await srvapi.call('install', [{sourceUrl: `file://${AUTH_APP1_PATH}`}])
  await srvapi.call('install', [{sourceUrl: `file://${AUTH_APP2_PATH}`}])

  const desc1 = await authApp1Api.call('createDb', [])
  t.is(typeof desc1.dbId, 'string', 'database created')
  t.is(desc1.tables.length, 1, '1 table registered')
  t.is(desc1.tables[0].tableId, 'auth-app.com/test', 'Test records table ID is correct')
  
  const desc2 = await authApp1Api.call('getDb', [])
  t.is(desc2.dbId, desc1.dbId, 'Same database returned')
  t.is(desc2.tables.length, 1, '1 table registered')
  t.is(desc2.tables[0].tableId, 'auth-app.com/test', 'Test records table ID is correct')

  const appDbs1 = await authApp1Api.call('listDbs', [])
  t.is(appDbs1.length, 1, 'One database')
  t.is(appDbs1[0].dbId, desc1.dbId, 'Is the database we created')

  const db = adb.db({}) // create db as system
  await db.isReady

  const appDbs2 = await authApp1Api.call('listDbs', [])
  t.is(appDbs2.length, 1, 'Still one database')
  t.is(appDbs2[0].dbId, desc1.dbId, 'Is still the database we created')

  const desc3 = await authApp2Api.call('createDb', [])
  t.is(typeof desc3.dbId, 'string', 'database created')
  t.is(desc3.tables.length, 1, '1 table registered')
  t.is(desc3.tables[0].tableId, 'auth-app.com/test', 'Test records table ID is correct')

  const appDbs3 = await authApp1Api.call('listDbs', [])
  t.is(appDbs3.length, 1, 'Still one database')
  t.is(appDbs3[0].dbId, desc1.dbId, 'Is still the database we created')

  const systemUserDbs1 = await adb.api.adminListDbsByOwningUser('system')
  t.is(systemUserDbs1.length, 2, 'System owns 2 dbs')

  const bobUserDbs1 = await adb.api.adminListDbsByOwningUser(user.key)
  t.is(bobUserDbs1.length, 2, 'Bob owns 2 dbs')

  t.is(await authApp1Api.call('listUserDbs', ['system']).then(() => true, () => false), false, 'bob cant list system dbs')

  const bobUserDbs2 = await authApp1Api.call('listUserDbs', [user.key])
  t.is(bobUserDbs2.length, 2, 'Bob owns 2 dbs')
})
