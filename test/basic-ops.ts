import test from 'ava'
import * as atek from '@atek-cloud/atek'
import * as path from 'path'
import { fileURLToPath } from 'url'

import { AtekDbRecordClient } from '@atek-cloud/node-rpc'
import AdbCtrlApiClient from '../src/gen/atek.cloud/adb-ctrl-api.js'
import AdbApiClient from '../src/gen/atek.cloud/adb-api.js'

const HERE_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), '..')

const adbCtrl = new AdbCtrlApiClient()
adbCtrl.$setEndpoint({port: 10000})
const adb = new AdbApiClient()
adb.$setEndpoint({port: 10000})

export const TEST_RECORD_ID = 'example.com/test'
export const TEST_RECORD_JSON_SCHEMA = {
  "$schema":"http://json-schema.org/draft-07/schema#",
  "type":"object",
  "properties":{
    "id":{"type":"string"},
    "obj":{"type":"object","properties":{"bool":{"type":"boolean"}}},
    "createdAt":{"type":"string","format":"date-time"}
  },
  "required":["id","createdAt"]
}
export const TEST_RECORD_TEMPLATES = {"table":{"title":"Test Records","description":"An example table."},"record":{"key":"{{/id}}","title":"Test record ID: {{/id}}"}};

export default interface TestRecord {
  id: string
  obj: {
    bool: boolean
  }
  createdAt: string
}

class TestRecordTable extends AtekDbRecordClient<TestRecord> {
  constructor(dbId?: string) {
    super(adb, dbId, TEST_RECORD_ID, undefined, TEST_RECORD_TEMPLATES, TEST_RECORD_JSON_SCHEMA)
  }
}

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

test('Register tables', async t => {
  const {dbId} = await adbCtrl.createDb({})
  t.truthy(dbId, 'DB successfully created')

  const testTable = new TestRecordTable(dbId)
  await testTable.register()

  const desc = await adb.describe(dbId)
  t.is(desc.dbId, dbId, 'Describe() for correct database')
  t.is(desc.tables.length, 1, '1 table registered')
  t.is(desc.tables[0].tableId, TEST_RECORD_ID, 'Test records table ID is correct')
  t.deepEqual(desc.tables[0].templates, TEST_RECORD_TEMPLATES, 'Test records table templates are correct')
  t.deepEqual(desc.tables[0].definition, TEST_RECORD_JSON_SCHEMA, 'Test records table schema is correct')
})

test('CRUD', async t => {
  const {dbId} = await adbCtrl.createDb({})
  t.truthy(dbId, 'DB successfully created')

  const testTable = new TestRecordTable(dbId)
  await testTable.register()

  for (let i = 0; i < 10; i++) {
    await testTable.create({id: `record${i}`, obj: {bool: i % 0 === 0}})
  }

  for (let i = 0; i < 10; i++) {
    const record = await testTable.get(`record${i}`)
    t.is(typeof record.seq, 'number', 'Record seq is set')
    t.is(record.key, `record${i}`, 'Record key is correct')
    t.is(record.path, `/${TEST_RECORD_ID}/record${i}`, 'Record path is correct')
    t.is(record.url, `hyper://${dbId}${record.path}`, 'Record url is correct')
    t.is(record.value?.id, record.key, 'Record value.id is correct')
    t.is(record.value?.obj.bool, i % 0 === 0, 'Record value.obj.bool is correct')
    t.is(typeof record.value?.createdAt, 'string', 'Record value.createdAt is set')
  }

  const {records} = await testTable.list()
  for (let i = 0; i < 10; i++) {
    const record = records[i]
    t.is(typeof record.seq, 'number', 'Record seq is set')
    t.is(record.key, `record${i}`, 'Record key is correct')
    t.is(record.path, `/${TEST_RECORD_ID}/record${i}`, 'Record path is correct')
    t.is(record.url, `hyper://${dbId}${record.path}`, 'Record url is correct')
    t.is(record.value.id, record.key, 'Record value.id is correct')
    t.is(record.value.obj.bool, i % 0 === 0, 'Record value.obj.bool is correct')
    t.is(typeof record.value?.createdAt, 'string', 'Record value.createdAt is set')
  }

  const record0 = await testTable.get('record0')
  await testTable.put(record0.key, Object.assign({}, record0.value, {obj: {bool: !record0.value.obj.bool}}))
  const record0b = await testTable.get('record0')
  t.is(record0b.value.obj.bool, !record0.value.obj.bool, 'Successfully put record')

  await testTable.delete('record0')
  t.falsy(await testTable.get('record0').catch(e => false), 'Successfully deleted record')
  t.is((await testTable.list()).records.length, 9, 'Successfully deleted record')
})


