import test from 'ava'
import * as atek from '@atek-cloud/atek'
import * as path from 'path'
import { fileURLToPath } from 'url'
import adb, { defineTable } from '@atek-cloud/adb-api'
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

  activeCfg = await inst.api('atek.cloud/inspect-api')('getConfig')
  t.truthy(activeCfg.serverDbId, 'Server DB ID was created')
})

test('Describe the server db', async t => {
  const desc = await adb.db(activeCfg.serverDbId).describe()
  t.truthy(desc, 'Returns a description object')
  t.is(desc.dbId, activeCfg.serverDbId, 'Gave the correct database\'s description')
  t.truthy(desc.tables.find((table: any) => table.tableId === 'atek.cloud/database'), 'Registered atek.cloud/database')
})

test('Create a new db', async t => {
  const db = await adb.db({displayName: 'Test DB 1'})
  await db.isReady
  t.truthy(db.dbId, 'DB successfully created')

  const desc = await db.describe()
  t.is(db.dbId, desc.dbId, 'Describe() for correct database')
  t.is(desc.displayName, 'Test DB 1')
})

test('Get and create a db by alias', async t => {
  const db = adb.db('dbalias', {displayName: 'Test DB 2'})
  await db.isReady
  t.truthy(db.dbId, 'DB successfully created')

  const desc = await db.describe()
  t.is(db.dbId, desc.dbId, 'Describe() for correct database')
  t.is(desc.displayName, 'Test DB 2', 'Display name is correct')

  const db2 = adb.db('dbalias', {displayName: 'Test DB 2'})
  await db2.isReady
  t.is(db.dbId, db2.dbId, 'DB successfully gotten')
})

test('Get and set db config', async t => {
  const db = adb.db('dbalias2', {
    displayName: 'Test DB 3',
    persist: true,
    presync: true
  })
  await db.isReady
  t.truthy(db.dbId, 'DB successfully created')

  const cfg = await adb.api.dbGetConfig('dbalias2')
  t.is(cfg.displayName, 'Test DB 3', 'Display name is correct')
  t.is(cfg.alias, 'dbalias2', 'Alias is correct')
  t.is(cfg.persist, true, 'Persist is correct')
  t.is(cfg.presync, true, 'Presync is correct')

  await adb.api.dbConfigure('dbalias2', {
    displayName: 'Test DB 3 - Modified',
    persist: false,
    presync: false
  })

  const cfg2 = await adb.api.dbGetConfig('dbalias2')
  t.is(cfg2.displayName, 'Test DB 3 - Modified', 'Display name is correct')
  t.is(cfg2.alias, 'dbalias2', 'Alias is correct')
  t.is(cfg2.persist, false, 'Persist is correct')
  t.is(cfg2.presync, false, 'Presync is correct')
})
