import test from 'ava'
import * as atek from '@atek-cloud/atek'
import * as path from 'path'
import { fileURLToPath } from 'url'
import adb from '@atek-cloud/adb-api'
adb.api.$setEndpoint({port: 10000})

const HERE_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), '..')

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

test.serial('Describe the server db', async t => {
  const desc = await adb.db(activeCfg.serverDbId).describe()
  t.truthy(desc, 'Returns a description object')
  t.is(desc.dbId, activeCfg.serverDbId, 'Gave the correct database\'s description')
})

test.serial('Create a new db', async t => {
  const db = await adb.db({})
  await db.isReady
  t.truthy(db.dbId, 'DB successfully created')

  const desc = await db.describe()
  t.is(db.dbId, desc.dbId, 'Describe() for correct database')
})

test.serial('Get and create a db by alias', async t => {
  const db = adb.db('dbalias', {})
  await db.isReady
  t.truthy(db.dbId, 'DB successfully created')

  const desc = await db.describe()
  t.is(db.dbId, desc.dbId, 'Describe() for correct database')

  const db2 = adb.db('dbalias', {})
  await db2.isReady
  t.is(db.dbId, db2.dbId, 'DB successfully gotten')
})

test.serial('Get and set db config', async t => {
  const db = adb.db('dbalias2', {
    access: 'private'
  })
  await db.isReady
  t.truthy(db.dbId, 'DB successfully created')

  const cfg = await adb.api.dbGetConfig('dbalias2')
  t.is(cfg.alias, 'dbalias2', 'Alias is correct')
  t.is(cfg.access, 'private', 'Access is correct')

  await adb.api.dbConfigure('dbalias2', {
    alias: 'dbalias2-modified',
    access: 'public'
  })

  const cfg2 = await adb.api.dbGetConfig('dbalias2-modified')
  t.is(cfg2.alias, 'dbalias2-modified', 'Alias is correct')
  t.is(cfg2.access, 'public', 'Access is correct')
})

test.serial('List all DBs', async t => {
  const dbs = await adb.api.adminListDbsByOwningUser('system')
  t.is(dbs.length, 4, 'List all databases gives all 4 created by previous tests')
  t.is(dbs.filter(db => db.isServerDb).length, 1, 'Only 1 server db')
  t.is(dbs.filter(db => !db.isServerDb && db.owner?.serviceKey === 'system').length, 3, '3 dbs owned by system')
})
