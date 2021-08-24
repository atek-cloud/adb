import test from 'ava'
import * as atek from '@atek-cloud/atek'
import * as path from 'path'
import { fileURLToPath } from 'url'

import AdbCtrlApiClient from '../src/gen/atek.cloud/adb-ctrl-api.js'
import AdbApiClient from '../src/gen/atek.cloud/adb-api.js'

const HERE_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), '..')

const adbCtrl = new AdbCtrlApiClient()
adbCtrl.$setEndpoint({port: 10000})
const adb = new AdbApiClient()
adb.$setEndpoint({port: 10000})

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
  const desc = await adb.describe(activeCfg.serverDbId)
  t.truthy(desc, 'Returns a description object')
  t.is(desc.dbId, activeCfg.serverDbId, 'Gave the correct database\'s description')
  t.truthy(desc.tables.find((table: any) => table.tableId === 'atek.cloud/database'), 'Registered atek.cloud/database')
})

test('Create a new db', async t => {
  const dbInfo = await adbCtrl.createDb({displayName: 'Test DB 1'})
  t.truthy(dbInfo.dbId, 'DB successfully created')

  const desc = await adb.describe(dbInfo.dbId)
  t.is(dbInfo.dbId, desc.dbId, 'Describe() for correct database')
  t.is(desc.displayName, 'Test DB 1')
})

test('Get and create a db by alias', async t => {
  const dbInfo = await adbCtrl.getOrCreateDb('dbalias', {displayName: 'Test DB 2'})
  t.truthy(dbInfo.dbId, 'DB successfully created')

  const desc = await adb.describe(dbInfo.dbId)
  t.is(dbInfo.dbId, desc.dbId, 'Describe() for correct database')
  t.is(desc.displayName, 'Test DB 2', 'Display name is correct')

  const dbInfo2 = await adbCtrl.getOrCreateDb('dbalias', {displayName: 'Test DB 2'})
  t.is(dbInfo.dbId, dbInfo2.dbId, 'DB successfully gotten')
})

test('Get and set db config', async t => {
  const dbInfo = await adbCtrl.getOrCreateDb('dbalias2', {
    displayName: 'Test DB 3',
    persist: true,
    presync: true
  })
  t.truthy(dbInfo.dbId, 'DB successfully created')

  const cfg = await adbCtrl.getDbConfig('dbalias2')
  t.is(cfg.displayName, 'Test DB 3', 'Display name is correct')
  t.is(cfg.alias, 'dbalias2', 'Alias is correct')
  t.is(cfg.persist, true, 'Persist is correct')
  t.is(cfg.presync, true, 'Presync is correct')

  await adbCtrl.configureDb('dbalias2', {
    displayName: 'Test DB 3 - Modified',
    persist: false,
    presync: false
  })

  const cfg2 = await adbCtrl.getDbConfig('dbalias2')
  t.is(cfg2.displayName, 'Test DB 3 - Modified', 'Display name is correct')
  t.is(cfg2.alias, 'dbalias2', 'Alias is correct')
  t.is(cfg2.persist, false, 'Persist is correct')
  t.is(cfg2.presync, false, 'Presync is correct')
})
