import test from 'ava'
import * as atek from '@atek-cloud/atek'
import * as path from 'path'
import { fileURLToPath } from 'url'
import adb, { defineTable } from '@atek-cloud/adb-api'
adb.api.$setEndpoint({port: 10000})

const HERE_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), '..')

export const TEST_RECORD_ID = 'example.com/test'
export const TEST_RECORD_DEFINITION = {
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

const testTable = defineTable<TestRecord>(TEST_RECORD_ID, {
  templates: TEST_RECORD_TEMPLATES,
  definition: TEST_RECORD_DEFINITION
})

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
  const db = adb.db({})
  await db.isReady
  t.truthy(db.dbId, 'DB successfully created')

  await testTable(db).isReady

  const desc = await db.describe()
  t.is(desc.dbId, db.dbId, 'Describe() for correct database')
  t.is(desc.tables.length, 1, '1 table registered')
  t.is(desc.tables[0].tableId, TEST_RECORD_ID, 'Test records table ID is correct')
  t.deepEqual(desc.tables[0].templates, TEST_RECORD_TEMPLATES, 'Test records table templates are correct')
  t.deepEqual(desc.tables[0].definition, TEST_RECORD_DEFINITION, 'Test records table schema is correct')
})

test('CRUD', async t => {
  const db = adb.db({})
  await db.isReady
  t.truthy(db.dbId, 'DB successfully created')

  for (let i = 0; i < 10; i++) {
    await testTable(db).create({id: `record${i}`, obj: {bool: i % 0 === 0}})
  }

  for (let i = 0; i < 10; i++) {
    const record = await testTable(db).get(`record${i}`)
    t.is(typeof record.seq, 'number', 'Record seq is set')
    t.is(record.key, `record${i}`, 'Record key is correct')
    t.is(record.path, `/${TEST_RECORD_ID}/record${i}`, 'Record path is correct')
    t.is(record.url, `hyper://${db.dbId}${record.path}`, 'Record url is correct')
    t.is(record.value?.id, record.key, 'Record value.id is correct')
    t.is(record.value?.obj.bool, i % 0 === 0, 'Record value.obj.bool is correct')
    t.is(typeof record.value?.createdAt, 'string', 'Record value.createdAt is set')
  }

  const {records} = await testTable(db).list()
  for (let i = 0; i < 10; i++) {
    const record = records[i]
    t.is(typeof record.seq, 'number', 'Record seq is set')
    t.is(record.key, `record${i}`, 'Record key is correct')
    t.is(record.path, `/${TEST_RECORD_ID}/record${i}`, 'Record path is correct')
    t.is(record.url, `hyper://${db.dbId}${record.path}`, 'Record url is correct')
    t.is(record.value.id, record.key, 'Record value.id is correct')
    t.is(record.value.obj.bool, i % 0 === 0, 'Record value.obj.bool is correct')
    t.is(typeof record.value?.createdAt, 'string', 'Record value.createdAt is set')
  }

  const record0 = await testTable(db).get('record0')
  await testTable(db).put(record0.key, Object.assign({}, record0.value, {obj: {bool: !record0.value.obj.bool}}))
  const record0b = await testTable(db).get('record0')
  t.is(record0b.value.obj.bool, !record0.value.obj.bool, 'Successfully put record')

  await testTable(db).delete('record0')
  t.falsy(await testTable(db).get('record0').catch((e: any) => false), 'Successfully deleted record')
  t.is((await testTable(db).list()).records.length, 9, 'Successfully deleted record')
})


