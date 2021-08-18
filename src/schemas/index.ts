import path from 'path'
import { promises as fsp } from 'fs'
import { fileURLToPath } from 'url'
import { Schema } from './schema.js'
import { createValidator } from './util.js'

const schemas = new Map()
const schemaValidator = createValidator({
  type: 'object',
  required: ['id', 'title', 'type'],
  properties: {
    id: {type: 'string'},
    rev: {type: 'number'},
    title: {type: 'string'},
    type: {type: 'string', enum: ['json-table']}
  }
})

// exported api
// =

export const getCached = schemas.get.bind(schemas)

export async function setup () {
}

export async function load (domain: string, name: string, minRevision: number = 1): Promise<Schema> {
  const id = `${domain}/${name}`
  return Promise.resolve(new Schema({id}))

  // TODO
  // if (schemas.get(id)?.rev >= minRevision) {
  //   return schemas.get(id)
  // }

  // let obj = await readSchemaFile(domain, name)
  // if (!obj || !isSchemaValid(obj) || obj.rev < minRevision) {
  //   try {
  //     await downloadSchemaFile(domain, name)
  //   } catch (e) {
  //     console.error(`Failed to download schema ${domain}/${name}`)
  //     console.error(e)
  //     throw e
  //   }
  //   obj = await readSchemaFile(domain, name)
  // }

  // try {
  //   schemaValidator.assert(obj)
  // } catch (e) {
  //   console.error(`Failed to load schema ${domain}/${name}`)
  //   console.error(e)
  //   throw e
  // }

  // if (obj.rev < minRevision && minRevision !== 1) {
  //   console.error(`Unable to find schema ${domain}/${name} that satisfies minimum revision ${minRevision}`)
  //   console.error(`Highest revision found: ${obj.rev}`)
  //   throw new Error(`Unable to find schema ${domain}/${name} that satisfies minimum revision ${minRevision}`)
  // }

  // schemas.set(id, new Schema(obj))
  // return schemas.get(id)
}

// internal methods
// =

async function readSchemaFile (domain: string, name: string): Promise<any> {
  try {
    // TODO
    // const installPath = (domain === 'ctzn.network' /* TEMP */)
    //   ? path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'schemas', `${name}.json`)
    //   : path.join(Config.getActiveConfig().schemaInstallPath(domain), `${name}.json`)
    // const str = await fsp.readFile(installPath, 'utf8')
    // return JSON.parse(str)
  } catch (e) {
    return undefined
  }
}

function isSchemaValid (obj: any): boolean {
  try {
    schemaValidator.assert(obj)
    return true
  } catch (e) {
    return false
  }
}

async function downloadSchemaFile (domain: string, name: string): Promise<void> {
  // TODO
  // const obj = await (await fetch(`https://${domain}/.well-known/selfcloud/${name}.json`)).json()
  // const installFolderPath = Config.getActiveConfig().schemaInstallPath(domain)
  // await fsp.mkdir(installFolderPath, {recursive: true})
  // await fsp.writeFile(path.join(installFolderPath, `${name}.json`), JSON.stringify(obj, null, 2), 'utf8')
}