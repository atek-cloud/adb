/* globals beaker */
import { html } from '../../../vendor/lit/lit.min.js'
import { repeat } from '../../../vendor/lit/directives/repeat.js'
import { BasePopup } from './base.js'
import * as session from '../../lib/session.js'
import { emit, readTextFile } from '../../lib/dom.js'
import selectorSvg from '../../icons/selector.js'
import { toKeyStr } from '../../lib/strings.js'
import { importFromJson } from '../../lib/import-export.js'
import '../button.js'

// exported api
// =

export class ImportdbPopup extends BasePopup {
  static get properties () {
    return {
      sourceType: {type: String},
      buckets: {type: Array},
      currentError: {type: String},
      currentStatus: {type: String}
    }
  }

  constructor (opts) {
    super()
    this.currentError = undefined
    this.currentStatus = undefined
    this.sourceType = 'file'
    this.buckets = []
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
    return BasePopup.create(ImportdbPopup, opts)
  }

  static destroy () {
    return BasePopup.destroy('adb-importdb-popup')
  }

  // rendering
  // =

  renderBody () {
    const sourceTypeNav = (id, label) => {
      let style = ''
      if (id === this.sourceType) {
        style = 'border-bottom-color: transparent;'
      } else {
        style = 'border-left-color: transparent; border-top-color: transparent; border-right-color: transparent;'
      }
      return html`
        <div class="border border-default rounded-t hover:bg-default-2 cursor-pointer pl-4 pr-5 py-2 font-medium" style=${style} @click=${e => {this.sourceType = id}}>${label}</div>
      `
    }
    return html`
      <form class="bg-default px-6 py-5" @submit=${this.onSubmit}>
        <div class="flex mb-4">
          ${sourceTypeNav('file', 'Import from File')}
          ${sourceTypeNav('url', 'Import from URL')}
          <div class="flex-1 border-b border-default"></div>
        </div>
        <div>
          ${this.sourceType === 'file' ? html`
            <label class="block" for="alias">Source File</label>
            <input class="block border border-default mb-3 px-4 py-3 rounded w-full" type="file" name="file" required>
          ` : html`
            <label class="block" for="alias">Source URL</label>
            <input class="block border border-default mb-3 px-4 py-3 rounded w-full" type="text" name="url" placeholder="hyper://" required>
          `}
        </div>
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
        ${this.currentStatus ? html`
          <div class="bg-default-3 px-4 py-3 mb-4">${this.currentStatus}</div>
        ` : ''}
        <div class="flex justify-between mt-6">
          <adb-button label="Cancel" @click=${this.onClickCancel}></adb-button>
          <adb-button primary btn-type="submit" label=${this.isNew ? 'Create' : 'Save'} ?disabled=${!!this.currentStatus} ?spinner=${!!this.currentStatus}></adb-button>
        </div>
      </form>
    `
  }

  // events
  // =

  onClickCancel (e) {
    if (this.currentStatus) {
      window.location.reload() // reload the page to force an abort. It's lazy but it works
    } else {
      this.onReject()
    }
  }

  async onSubmit (e) {
    e.preventDefault()
    this.currentError = undefined
    const sourceUrl = e.currentTarget.url?.value
    const sourceFile = e.currentTarget.file?.files?.[0]
    const serviceKey = e.currentTarget.serviceKey.value
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

    let records
    if (this.sourceType === 'file') {
      this.currentStatus = 'Reading file...'
      const fileStr = await readTextFile(sourceFile)
      try {
        records = importFromJson(fileStr)
      } catch (e) {
        this.currentStatus = undefined
        this.currentError = e.toString()
        return
      }
    } else if (this.sourceType === 'url') {
      const dbId = toKeyStr(sourceUrl)
      if (!dbId) {
        this.currentStatus = undefined
        this.currentError = `Invalid URL. Must give an Atek DB URL.`
        return
      }
      this.currentStatus = 'Reading target database, this may take a moment...'
      records = (await session.adb.recordList(dbId, '/')).records
    }

    this.currentStatus = 'Populating new database...'
    const dbInfo = await session.adb.adminCreateDb({
      alias,
      access,
      owner: {serviceKey}
    })
    for (const record of records) {
      await session.adb.recordPut(dbInfo.dbId, record.path, record.value)
    }
    emit(this, 'navigate-to', {detail: {url: `/p/db/${dbInfo.dbId}`}})
  }
}

customElements.define('adb-importdb-popup', ImportdbPopup)

async function isAliasAvailable (serviceKey, alias) {
  const dbs = [
    ...(await session.adb.adminListDbsByOwningUser('system').catch(e => ([]))),
    ...(await session.adb.adminListDbsByOwningUser().catch(e => ([])))
  ]
  return !dbs.find(db => {
    return db.owner?.serviceKey === serviceKey && db.alias === alias
  })
}