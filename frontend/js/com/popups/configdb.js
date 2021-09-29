/* globals beaker */
import { html } from '../../../vendor/lit/lit.min.js'
import { repeat } from '../../../vendor/lit/directives/repeat.js'
import { BasePopup } from './base.js'
import * as session from '../../lib/session.js'
import { emit } from '../../lib/dom.js'
import selectorSvg from '../../icons/selector.js'
import '../button.js'

// exported api
// =

export class ConfigdbPopup extends BasePopup {
  static get properties () {
    return {
      buckets: {type: Array},
      currentError: {type: String},
      access: {type: String}
    }
  }

  constructor (opts) {
    super()
    this.currentError = undefined
    this.buckets = []
    this.isNew = !opts?.dbId
    this.dbId = opts?.dbId
    this.serviceKey = opts?.serviceKey
    this.alias = opts?.alias
    this.access = opts?.access || 'public'
    this.load()
  }

  async load () {
    const appInfo = await session.frontend.getAppInfo()
    const services = (await session.frontend.listServices()).services
    this.buckets = [
      {serviceKey: appInfo.serviceKey, displayName: 'My Databases'},
      ...services.filter(s => !s.settings.id.startsWith('core.')).map(s => ({serviceKey: s.key, displayName: `${s.settings?.manifest?.name || s.settings?.id || s.key}`}))
    ]
  }

  get shouldShowHead () {
    return false
  }

  get shouldCloseOnOuterClick () {
    return false
  }

  get maxWidth () {
    return '500px'
  }

  firstUpdated () {
    this.querySelector('input[name=alias]')?.focus()
  }

  // management
  //

  static async create (opts) {
    return BasePopup.create(ConfigdbPopup, opts)
  }

  static destroy () {
    return BasePopup.destroy('adb-configdb-popup')
  }

  // rendering
  // =

  renderBody () {
    return html`
      <form class="bg-default px-6 py-5" @submit=${this.onSubmit}>
        <h2 class="text-2xl mb-2 font-medium">${this.isNew ? 'New' : 'Edit'} Database</h2>
        ${this.serviceKey ? '' : html`
          <div>
            <label class="block" for="alias">Bucket</label>
            <div class="flex items-center border border-default mb-3 px-4 py-3 rounded">
              <select class="flex-1 appearance-none outline-none" name="serviceKey">
                ${repeat(this.buckets, b => b.serviceKey, b => html`
                  <option value=${b.serviceKey} ?selected=${this.serviceKey === b.serviceKey}>${b.displayName}</option>
                `)}
              </select>
              ${selectorSvg()}
            </div>
          </div>
        `}
        <div>
          <label class="block" for="alias">Database Name</label>
          <input class="block border border-default mb-3 px-4 py-3 rounded w-full" type="text" name="alias" value=${this.alias || ''} required>
        </div>
        <div>
          <label class="block" for="alias">Access</label>
          <div class="flex items-center border border-default mb-1 px-4 py-3 rounded">
            <select class="flex-1 appearance-none outline-none" name="access" @change=${this.onAccessChange}>
              <option value="public" ?selected=${this.access === 'public'}>Public</option>
              <option value="private" ?selected=${this.access === 'private'}>Private</option>
            </select>
            ${selectorSvg()}
          </div>
          <div class="text-default-3 text-sm mb-4">
            ${this.access === 'public' ? 'Other users can access the database over the network.' : ''}
            ${this.access === 'private' ? 'Only you can access the database.' : ''}
          </div>
        </div>
        ${this.currentError ? html`
          <div class="bg-error text-error px-4 py-3 mb-4">${this.currentError}</div>
        ` : ''}
        <div class="flex justify-between mt-6">
          <adb-button label="Cancel" @click=${this.onReject}></adb-button>
          <adb-button primary btn-type="submit" label=${this.isNew ? 'Create' : 'Save'}></adb-button>
        </div>
      </form>
    `
  }

  // events
  // =

  onAccessChange (e) {
    this.access = e.currentTarget.value
  }

  async onSubmit (e) {
    e.preventDefault()
    this.currentError = undefined
    const serviceKey = this.serviceKey || e.currentTarget.serviceKey.value
    const alias = e.currentTarget.alias.value.trim()
    const access = e.currentTarget.access.value
    if (!alias) {
      this.currentError = 'Please specify a name for your database'
      return this.requestUpdate()
    }
    if (alias !== this.alias && !(await isAliasAvailable(serviceKey, alias))) {
      this.currentError = `A database named ${alias} already exists in this bucket`
      return this.requestUpdate()
    }
    if (this.isNew) {
      const dbInfo = await session.adb.adminCreateDb({
        alias,
        access,
        owner: {serviceKey}
      })
      emit(this, 'navigate-to', {detail: {url: `/p/db/${dbInfo.dbId}`}})
    } else {
      await session.adb.adminEditDbConfig(this.dbId, {alias, access})
    }
    this.onResolve()
  }
}

customElements.define('adb-configdb-popup', ConfigdbPopup)

async function isAliasAvailable (serviceKey, alias) {
  const dbs = [
    ...(await session.adb.adminListDbsByOwningUser('system').catch(e => ([]))),
    ...(await session.adb.adminListDbsByOwningUser().catch(e => ([])))
  ]
  return !dbs.find(db => {
    return db.owner?.serviceKey === serviceKey && db.alias === alias
  })
}