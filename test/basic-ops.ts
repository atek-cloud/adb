import test from 'ava'
import * as atek from '@atek-cloud/atek'
import * as path from 'path'
import { fileURLToPath } from 'url'
import adb, { defineSchema } from '@atek-cloud/adb-api'
adb.api.$setEndpoint({port: 10000})

const HERE_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), '..')

const TEST_RECORD_ID = 'example.com/test'
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
const TEST_RECORD_PKEY = '/id'

interface TestRecord {
  id: string
  obj: {
    bool: boolean
  }
  createdAt: string
}

const testTable = defineSchema<TestRecord>(TEST_RECORD_ID, {
  pkey: TEST_RECORD_PKEY,
  jsonSchema: TEST_RECORD_DEFINITION
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
  adb.api.$setAuthHeader(`Bearer ${inst.authToken}`)

  activeCfg = await inst.api('atek.cloud/inspect-api').call('getConfig')
  t.truthy(activeCfg.serverDbId, 'Server DB ID was created')
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


