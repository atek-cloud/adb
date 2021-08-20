import createMlts from 'monotonic-lexicographic-timestamp'
import { TableDescription, TableSettings, TableTemplates } from '../gen/atek.cloud/adb-api.js'
// import { ValidationError } from '../lib/errors.js'
import { createTemplateFn, TemplateFunction, createValidator, Validator } from './util.js'

const mlts = createMlts()

interface TableTemplateFns {
  table: {
    title: TemplateFunction
    description: TemplateFunction
  }
  record: {
    key: TemplateFunction
    title: TemplateFunction
    description: TemplateFunction
  }
}

export class TableSchema implements TableDescription {
  tableId: string
  revision?: number
  templates?: TableTemplates
  definition?: object
  gen: TableTemplateFns
  recordValidator?: Validator

  constructor (tableId: string, settings: TableSettings) {
    this.tableId = tableId
    this.revision = settings.revision
    this.templates = settings.templates
    this.definition = settings.definition

    this.gen = {
      table: {
        title: createTemplateFn(this.templates?.table?.title, value => tableId),
        description: createTemplateFn(this.templates?.table?.title, value => undefined)
      },
      record: {
        key: createTemplateFn(this.templates?.table?.title, value => mlts()),
        title: createTemplateFn(this.templates?.table?.title, value => undefined),
        description: createTemplateFn(this.templates?.table?.title, value => undefined)
      }
    }

    if (this.definition) {
      this.recordValidator = createValidator(this.definition)
    }
  }

  validate (value: object): boolean {
    if (this.recordValidator) {
      return this.recordValidator.validate(value)
    }
    return true
  }

  assertValid (value: object) {
    if (this.recordValidator) {
      return this.recordValidator.assert(value)
    }
  }

  assertBlobMimeTypeValid (blobName: string, mimeType: string | undefined) {
    // TODO
  }

  assertBlobSizeValid (blobName: string, len: number) {
    // TODO
  }

  // TODO
  /*
  constructor (obj) {
    this.id = obj.id
    this.schemaObject = obj
    this.validate = undefined
    this.validateParams = undefined
    this.keyTemplate = undefined
    this.shell = {
      descTemplate: undefined
    }

    const failure = (msg, e) => {
      console.error(msg, this.id)
      console.error(e)
      process.exit(1)
    }

    try {
      if (this.schemaObject.definition) {
        this.validate = ajv.compile(this.schemaObject.definition)
      }
    } catch (e) { failure('Failed to compile schema definition', e) }
    try {
      if (this.schemaObject.keyTemplate) {
        this.keyTemplate = generateKeyTemplate(this.schemaObject.keyTemplate)
      }
    } catch (e) { failure('Failed to compile schema keyTemplate', e) }
    try {
      if (this.schemaObject.shell?.descTemplate) {
        this.shell.descTemplate = generateKeyTemplate(this.schemaObject.shell.descTemplate)
      }
    } catch (e) { failure('Failed to compile schema shell.descTemplate', e) }
    try {
      if (this.schemaObject.parameters) {
        this.validateParams = ajv.compile(this.schemaObject.parameters)
      }
    } catch (e) { failure('Failed to compile schema parameters', e) }

    if (this.schemaObject.type === 'json-table') {
      // no further setup needed
    } else if (this.schemaObject.type === 'json-view' || this.schemaObject.type === 'blob-view') {
      // no further setup needed
    } else if (this.schemaObject.type === 'method') {
      // no further setup needed
    } else {
      console.error('Unknown table type:', this.schemaObject.type)
    }
  }

  get domain () {
    return this.id.split('/')[0]
  }

  get name () {
    return this.id.split('/')[1]
  }

  get rev () {
    return this.schemaObject?.rev || 1
  }

  generateKey (value) {
    if (!this.keyTemplate) {
      throw new Error(`Unable to generate key for ${this.id} record, no keyTemplate specified`)
    }
    return this.keyTemplate.map(fn => fn(value)).join('')
  }

  generateShellDesc (value) {
    if (this.shell.descTemplate) {
      return this.shell.descTemplate.map(fn => fn(value)).join('')
    }
  }

  get hasCreatedAt () {
    return (
      this.schemaObject.type === 'json-table'
      && this.schemaObject.definition
      && (
        this.schemaObject.definition.properties?.createdAt
        || this.schemaObject.definition.oneOf?.every?.(obj => obj.properties.createdAt)
      )
    )
  }

  assertValid (value) {
    const valid = this.validate(value)
    if (!valid) {
      throw new ValidationError(this.validate.errors[0])
    }
  }

  assertBlobMimeTypeValid (blobName, mimeType) {
    const def = this.schemaObject?.blobs?.[blobName]
    if (!def) {
      throw new ValidationError(`Invalid blob name: ${blobName}`)
    }
    if (def.mimeTypes && !def.mimeTypes.includes(mimeType)) {
      throw new ValidationError(`Blob mime-type (${mimeType}) is invalid, must be one of ${def.mimeTypes.join(', ')}`)
    }
  }

  assertBlobSizeValid (blobName, size) {
    const def = this.schemaObject?.blobs?.[blobName]
    if (!def) {
      throw new ValidationError(`Invalid blob name: ${blobName}`)
    }
    if (def.maxSize && size > def.maxSize) {
      throw new ValidationError(`Blob size (${size}) is larger than allowed (${def.maxSize})`)
    }
  }*/
}

/*
TODO
export function compileKeyGenerator (keyTemplate) {
  const keyTemplateFns = generateKeyTemplate(keyTemplate)
  return value => keyTemplateFns.map(fn => fn(value)).join('')
}

function generateKeyTemplate (keyTemplate) {
  return keyTemplate.map(segment => {
    if (segment.type === 'json-pointer') {
      if (typeof segment.value !== 'string') {
        throw new Error('"json-pointer" must have a value')
      }
      const ptr = JsonPointer.create(segment.value)
      return (record) => {
        let value = ptr.get(record)
        if (!VALID_PTR_RESULT_TYPES.includes(typeof value)) {
          throw new Error(`Unable to generate key, ${segment.value} found type ${typeof value}`)
        }
        return value
      }
    } else if (segment.type === 'auto') {
      return (record) => mlts()
    } else if (segment.type === 'string') {
      if (typeof segment.value !== 'string') {
        throw new Error('"string" must have a value')
      }
      return (record) => segment.value
    } else {
      throw new Error(`Unknown keyTemplate segment type: "${segment.type}"`)
    }
  })
}
*/