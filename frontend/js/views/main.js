import { LitElement, html } from '../../vendor/lit/lit.min.js'
import { repeat } from '../../vendor/lit/directives/repeat.js'
import * as session from '../lib/session.js'
import { shortenHash } from '../lib/strings.js'
import { writeToClipboard } from '../lib/clipboard.js'
import { exportAsJson } from '../lib/import-export.js'
import { ConfirmPopup } from '../com/popups/confirm.js'
import { ConfigdbPopup } from '../com/popups/configdb.js'
import { ImportdbPopup } from '../com/popups/importdb.js'
import * as toast from '../com/toast.js'
import '../com/button.js'

class MainView extends LitElement {
  static get properties () {
    return {
      currentPath: {type: String, attribute: 'current-path'},
      bucketId: {type: String},
      buckets: {type: Object}
    }
  }

  createRenderRoot() {
    return this // dont use shadow dom
  }

  constructor () {
    super()
    this.bucketId = 'root'
  }

  async load () {
    document.title = `Databases`

    const appInfo = await session.frontend.getAppInfo()
    const services = (await session.frontend.listServices()).services
    const dbs = [
      ...(await session.adb.adminListDbsByOwningUser('system').catch(e => ([]))),
      ...(await session.adb.adminListDbsByOwningUser().catch(e => ([])))
    ]
    this.appInfo = appInfo
    this.buckets = toBuckets(dbs, services, appInfo)
    console.log(this.buckets, dbs, services, appInfo)
  }

  async refresh () {
  }

  async pageLoadScrollTo (y) {
  }

  // rendering
  // =

  render () {
    return html`
      <main class="flex min-h-screen bg-default-3">
        <div class="flex-1">
          <div class="px-4 py-2 text-sm bg-default border-b border-default-2">
            <a class="font-medium hover:underline" href="/">Atek DB</a>
          </div>
          ${this.buckets ? html`
            ${repeat(sortBuckets(this.appInfo, Object.values(this.buckets)), b=>b.id, (bucket) => html`
              <div class="mx-2 my-2 border border-default rounded bg-default">
                <div class="px-4 py-2 font-medium text-sm">
                  ${bucket.displayName}
                </div>
              <div class="flex px-4 py-2 text-xs font-bold border-t border-default-2">
                <div style="flex: 0 0 120px">ID</div>
                <div class="flex-1">Alias</div>
                <div style="flex: 0 0 140px">Writable</div>
                <div style="flex: 0 0 140px">Access</div>
                <div style="flex: 0 0 140px">Actions</div>
              </div>
                ${repeat(bucket.dbs, db=>db.dbId, (db, i) => html`
                  <a class="flex items-center px-4 py-2 hover:bg-default-2 text-sm border-t border-default-2" href=${`/p/db/${db.dbId}`}>
                    <div style="flex: 0 0 120px">${shortenHash(db.dbId)}</div>
                    <div class="flex-1">${db.alias}</div>
                    <div style="flex: 0 0 140px">${db.writable ? 'writable' : 'read-only'}</div>
                    <div style="flex: 0 0 140px">${db.access}</div>
                    <div style="flex: 0 0 140px">
                      ${!db.isServerDb ? html`
                        <adb-button transparent data-tooltip="Settings" icon="fas fa-cogs" @click=${e => this.onClickEditDatabaseSettings(e, bucket, db)}></adb-button>
                        <adb-button transparent data-tooltip="Copy URL" icon="fas fa-link" @click=${e => this.onClickCopyDatabaseLink(e, db)}></adb-button>
                        <adb-button transparent data-tooltip="Export as File" icon="fas fa-file-download" @click=${e => this.onClickExportDatabase(e, db)}></adb-button>
                        <adb-button transparent data-tooltip="Delete" icon="far fa-trash-alt" @click=${e => this.onClickDeleteDatabase(e, db)}></adb-button>
                      ` : html`
                        <adb-button transparent data-tooltip="Copy URL" icon="fas fa-link" @click=${e => this.onClickCopyDatabaseLink(e, db)}></adb-button>
                        <adb-button transparent data-tooltip="Export as File" icon="fas fa-file-download" @click=${e => this.onClickExportDatabase(e, db)}></adb-button>
                      `}
                    </div>
                  </a>
                `)}
                </div>
            `)}
          ` : html`
            <div class="spinner"></div>
          `}
        </div>
        <div class="px-4 py-3 border-default-2 border-l" style="flex: 0 0 250px">
          <adb-button btn-class="block w-full py-1.5 px-3 mb-2" color="green" label="New Database" @click=${this.onClickNewDatabase}></adb-button>
          <adb-button btn-class="block w-full py-1.5 px-3 mb-2" label="Import Database" @click=${this.onClickImportDatabase}></adb-button>
        </div>
      </main>
    `
  }

  // events
  // =

  async onClickNewDatabase (e) {
    e.preventDefault()
    e.stopPropagation()
    await ConfigdbPopup.create({})
  }

  async onClickImportDatabase (e) {
    e.preventDefault()
    e.stopPropagation()
    await ImportdbPopup.create({})
  }

  async onClickEditDatabaseSettings (e, bucket, db) {
    e.preventDefault()
    e.stopPropagation()
    await ConfigdbPopup.create({
      dbId: db.dbId,
      serviceKey: bucket.id,
      alias: db.alias,
      access: db.access
    })
    this.load()
  }

  async onClickExportDatabase (e, db) {
    e.preventDefault()
    e.stopPropagation()
    const records = await session.adb.recordsList(db.dbId, '/')
    exportAsJson(records)
  }

  onClickCopyDatabaseLink (e, db) {
    e.preventDefault()
    e.stopPropagation()
    writeToClipboard(`hyper://${db.dbId}/`)
    toast.create('URL copied to clipboard')
  }

  async onClickDeleteDatabase (e, db) {
    e.preventDefault()
    e.stopPropagation()
    await ConfirmPopup.create({
      message: 'Are you sure you want to delete this database?',
      help: 'Your applications may depend on this database'
    })
    try {
      await session.adb.adminDeleteDb(db.dbId)
    } catch (e) {
      toast.create(`Failed to delete: ${e.toString()}`, 'error')
      console.error(e)
      return
    }
    toast.create('Database deleted from bucket')
    this.load()
  }
}

customElements.define('adb-main-view', MainView)

class Bucket {
  constructor (id, displayName) {
    this.id = id
    this.displayName = displayName
    this.dbs = []
  }
}

function toBuckets (dbs, services, appInfo) {
  const buckets = {}

  const add = (bucketId, bucketDisplayName, db) => {
    if (!buckets[bucketId]) {
      buckets[bucketId] = new Bucket(bucketId, bucketDisplayName)
    }
    if (!buckets[bucketId].dbs.find(db2 => db2.dbId === db.dbId)) {
      buckets[bucketId].dbs.push(db)
    }
  }

  const getService = serviceKey => services.find(s => s.key === serviceKey)
  const getDb = (db, alias) => {
    alias = alias || db.alias
    return {
      dbId: db.dbId,
      isServerDb: db.isServerDb,
      alias,
      writable: db.writable,
      access: db.access
    }
  }

  for (const db of dbs) {
    if (db.isServerDb) {
      add('system', 'System Databases', getDb(db, 'server'))
    } else {
      if (db.owner.serviceKey === appInfo.serviceKey) {
        add(appInfo.serviceKey, 'My Databases', getDb(db))
      } else {
        const s = getService(db.owner.serviceKey)
        if (s) {
          const name = `${s.settings?.manifest?.name || s.settings?.id || s.key}`
          add(db.owner.serviceKey, name, getDb(db))
        } else {
          add('trash', 'Trash', getDb(db))
        }
      }
    }
  }

  return buckets
}

function sortBuckets (appInfo, buckets) {
  buckets.sort((a, b) => {
    if (a.id === 'system') return -1
    if (b.id === 'system') return 1
    if (a.id === appInfo.serviceKey) return -1
    if (b.id === appInfo.serviceKey) return 1
    return b.displayName.localeCompare(a.displayName)
  })
  return buckets
}
