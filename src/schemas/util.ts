import Ajv from 'ajv'
import addFormats from 'ajv-formats'
import { JsonPointer } from 'json-ptr'
import { ValidationError } from '../lib/errors.js'

const VALID_PTR_RESULT_TYPES = ['number', 'string', 'boolean']
export const ajv = new Ajv({strictTuples: false})
addFormats(ajv)

export interface Validator {
  validate: (v: any) => boolean
  assert: (v: any) => void
}

export interface TemplateFunction {
  (value: object): string
}

export function createValidator (schema: object): Validator {
  const validate = ajv.compile(schema)
  return {
    validate: (value: any) => validate(value),
    assert: (value: any) => {
      const valid = validate(value)
      if (!valid) {
        throw new ValidationError(`${validate.errors?.[0].propertyName} ${validate.errors?.[0].message}`)
      }
    }
  }
}

export function createTemplateFn (template: string|undefined, defFn: TemplateFunction): TemplateFunction {
  if (!template) return defFn
  const parts = template.split(/(\{\{|\}\})/g)
  if (parts.length % 2 !== 1) {
    throw new Error('Invalid template: unbalanced {{ }} brackets')
  }
  const fns = parts.map((part, i) => {
    if (i % 2 === 0) return (value: object) => part
    const ptr = JsonPointer.create(part)
    return (value: object) => {
      const res = ptr.get(value)
      if (!VALID_PTR_RESULT_TYPES.includes(typeof res)) {
        throw new Error(`Unable to generate key, ${part} found type ${typeof res}`)
      }
      return value
    }
  })
  return (value: object) => fns.map(fn => fn(value)).join('')
}