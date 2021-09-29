import Ajv from 'ajv'
import addFormats from 'ajv-formats'
import { ValidationError } from '../lib/errors.js'
import { DATABASE, USER, SERVICE } from '@atek-cloud/adb-tables'

export * from '@atek-cloud/adb-tables'

export const DB_PATH = DATABASE.ID.split('/')
export const USER_PATH = USER.ID.split('/')
export const SERVICE_PATH = SERVICE.ID.split('/')

export const ajv = new Ajv({strictTuples: false})
addFormats(ajv)

export const dbValidator = createValidator(DATABASE.DEFINITION)
export const userValidator = createValidator(USER.DEFINITION)
export const serviceValidator = createValidator(USER.DEFINITION)

export interface Validator {
  errors: () => any
  validate: (v: any) => boolean
  assert: (v: any) => void
}

export function createValidator (schema: object): Validator {
  const validate = ajv.compile(schema)
  return {
    errors: () => validate.errors,
    validate: (value: any) => validate(value),
    assert: (value: any) => {
      const valid = validate(value)
      if (!valid) {
        const what = validate.errors?.[0].propertyName || validate.errors?.[0].instancePath
        throw new ValidationError(`${what} ${validate.errors?.[0].message}`)
      }
    }
  }
}
