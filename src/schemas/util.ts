import Ajv from 'ajv'
import addFormats from 'ajv-formats'
import { ValidationError } from '../lib/errors.js'

export const ajv = new Ajv({strictTuples: false})
addFormats(ajv)

export interface Validator {
  validate: (v: any) => boolean
  assert: (v: any) => void
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

