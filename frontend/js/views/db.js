import { LitElement, html } from '../../vendor/lit/lit.min.js'
import { repeat } from '../../vendor/lit/directives/repeat.js'
import * as session from '../lib/session.js'
import { joinPath, shortenHash, pluralize } from '../lib/strings.js'
import { emit, readTextFile } from '../lib/dom.js'
import { exportAsJson, importFromJson } from '../lib/import-export.js'
import * as toast from '../com/toast.js'
import { GeneralPopup } from '../com/popups/general.js'
import { ConfirmPopup } from '../com/popups/confirm.js'
import { ConfigdbPopup } from '../com/popups/configdb.js'
import '../com/button.js'
import '../com/code-textarea.js'

class DbView extends LitElement {
  static get properties () {
    return {
      currentPath: {type: String, attribute: 'current-path'},
      error: {type: String},
      record: {type: Array},
      records: {type: Array},
      selectedRecordKeys: {type: Array},
      hasChanges: {type: Boolean},
      isEditingNew: {type: Object}
    }
  }

  createRenderRoot() {
    return this // dont use shadow dom
  }

  constructor () {
    super()
    this.currentPath = ''
    this.hyperKey = ''
    this.dbDesc = undefined
    this.error = undefined
    this.record = undefined
    this.records = undefined
    this.selectedRecordKeys = []
    this.initialRecordValue = undefined
    this.hasChanges = false
    this.isEditingNew = undefined
  }

  async load () {
    document.title = `Data View`

    this.record = undefined
    this.records = undefined
    this.selectedRecordKeys = []
    this.hasChanges = false
    this.isEditingNew = undefined
    const pathParts = this.currentPath.split('/').filter(Boolean)
    this.hyperKey = pathParts[2]

    if (!this.dbDesc) {
      this.dbDesc = await session.adb.dbDescribe(this.hyperKey)
      console.log(this.dbDesc)
    }

    const qs = new URLSearchParams(location.search)
    if (qs.has('new')) {
      this.isEditingNew = {
        path: qs.get('path').split('/').filter(Boolean)
      }
      this.currentDBPath = this.isEditingNew.path
      console.log(this.isEditingNew)
    } else {
      this.currentDBPath = pathParts.slice(3)
      try {
        this.record = qs.has('container') ? undefined : await session.adb.recordGet(this.hyperKey, this.currentDBPath).catch(e => undefined)
        this.records = toShallow(this.currentDBPath, (await session.adb.recordList(this.hyperKey, this.currentDBPath)).records)
        this.initialRecordValue = this.record ? JSON.stringify(this.record.value, null, 2) : undefined
        console.log(this.currentDBPath, this.record || this.records)
      } catch (e) {
        this.error = e.toString()
        console.log('Failed to fetch records')
        console.log(e)
      }
    }
  }

  async refresh () {
  }

  async pageLoadScrollTo (y) {
  }

  async doRenamePopup (record) {
    const newpath = await GeneralPopup.create({
      render () {
        const onSubmit = e => {
          e.preventDefault()
          const newpath = e.currentTarget.path.value.trim()
          if (!newpath) {
            return toast.create('Path can not be empty', 'error')
          }
          this.dispatchEvent(new CustomEvent('resolve', {detail: newpath}))
        }
        return html`
          <form class="bg-default px-6 py-4 rounded" @submit=${onSubmit}>
            <h2 class="text-2xl mb-2 font-medium">Rename record</h2>
            <input
              type="text"
              name="path"
              class="block border rounded px-5 py-4 w-full shadow-inner mb-4"
              value=${record.path}
            >
            <div class="flex justify-between">
              <adb-button label="Cancel" @click=${this.onReject}></adb-button>
              <adb-button primary label="Save" btn-type="submit"></adb-button>
            </div>
          </form>
        `
      }
    })
    if (newpath === record.path) {
      return
    }
    try {
      await doRename(this.hyperKey, record, newpath)
    } catch (e) {
      console.error(e)
      toast.create(`Failed to rename record: ${e.toString()}`, 'error')
      return
    }
    emit(this, 'navigate-to', {detail: {url: `/p/db/${joinPath(this.hyperKey, newpath)}`}})
  }

  async doClonePopup (record) {
    const newpath = await GeneralPopup.create({
      render () {
        const onSubmit = e => {
          e.preventDefault()
          const newpath = e.currentTarget.path.value
          if (!newpath) {
            return toast.create('Path can not be empty', 'error')
          }
          if (newpath === record.path) {
            return toast.create('Must choose a new path', 'error')
          }
          this.dispatchEvent(new CustomEvent('resolve', {detail: newpath}))
        }
        return html`
          <form class="bg-default px-6 py-4 rounded" @submit=${onSubmit}>
            <h2 class="text-2xl mb-2 font-medium">Clone record</h2>
            <div>Clone:</div>
            <input
              type="text"
              disabled
              class="block border rounded px-5 py-4 w-full shadow-inner mb-4"
              value=${record.path}
            >
            <div>To:</div>
            <input
              type="text"
              name="path"
              class="block border rounded px-5 py-4 w-full shadow-inner mb-4"
              value=${record.path}
            >
            <div class="flex justify-between">
              <adb-button label="Cancel" @click=${this.onReject}></adb-button>
              <adb-button primary label="Save" btn-type="submit"></adb-button>
            </div>
          </form>
        `
      }
    })
    try {
      await doClone(this.hyperKey, record, newpath)
    } catch (e) {
      console.error(e)
      toast.create('Failed to create new record', 'error')
      return
    }
    emit(this, 'navigate-to', {detail: {url: `/p/db/${joinPath(this.hyperKey, newpath)}`}})
  }

  async doImportPopup () {
    const currentPath = `/${this.currentDBPath.join('/')}`
    const {path, file} = await GeneralPopup.create({
      render () {
        const onSubmit = e => {
          e.preventDefault()
          const file = e.currentTarget.file.files[0]
          const path = e.currentTarget.path.value.trim()
          if (!path) {
            return toast.create('Path can not be empty', 'error')
          }
          this.dispatchEvent(new CustomEvent('resolve', {detail: {path, file}}))
        }
        return html`
          <form class="bg-default px-6 py-4 rounded" @submit=${onSubmit}>
            <h2 class="text-2xl mb-2 font-medium">Import records</h2>
            <label for="file">Source file</label>
            <input class="block border border-default mb-3 px-5 py-4 rounded w-full" type="file" name="file" required>
            <label for="path">Target path</label>
            <input
              type="text"
              name="path"
              class="block border rounded px-5 py-4 w-full shadow-inner mb-4"
              value=${currentPath}
            >
            <div class="text-sm text-default-3 mb-4">Note: records may be overwritten.</div>
            <div class="flex justify-between">
              <adb-button label="Cancel" @click=${this.onReject}></adb-button>
              <adb-button primary label="Import" btn-type="submit"></adb-button>
            </div>
          </form>
        `
      }
    })
    try {
      toast.create('Importing, please wait...')
      const n = await doImport(this.hyperKey, path, file)
      toast.create(`Imported ${n} ${pluralize(n, 'record')}`, 'success')
    } catch (e) {
      console.error(e)
      toast.create(`Failed to import records: ${e.toString()}`, 'error')
      return
    }
    this.load()
  }

  // rendering
  // =

  render () {
    return html`
      <main class="flex min-h-screen bg-default-3">
        <div class="flex-1" style="min-width: 0">
          <div class="px-4 py-2 text-sm bg-default border-b border-default-2">
            <a class="font-medium hover:underline" href="/">Atek DB</a>
            ??? <a class="font-medium hover:underline" href="/p/db/${this.hyperKey}">${this.dbDesc?.alias || shortenHash(this.hyperKey)}</a>
          </div>
          <div class="px-4 py-4">
            <div class=" mb-2">
              ${this.renderBreadcrumbs()}
            </div>
            ${this.renderCurrentData()}
          </div>
        </div>
        <div class="px-4 py-3 border-default-2 border-l" style="flex: 0 0 250px">
          <adb-button btn-class="block w-full py-1.5 px-3 mb-2" color="green" label="New Database" @click=${this.onClickNewDatabase}></adb-button>
          <adb-button btn-class="block w-full py-1.5 px-3 mb-2" label="Import Database" @click=${this.onClickImportDatabase}></adb-button>
        </div>
      </main>
    `
  }

  renderBreadcrumbs () {
    const htmlAcc = []
    htmlAcc.push(html`
      <a class="px-2 py-1 hover:text-primary" href="/p/db/${this.hyperKey}/">Root</a>
    `)
    if (this.currentDBPath?.length) {
      const acc = []
      for (const seg of this.currentDBPath) {
        htmlAcc.push(html`
          <div class="relative" style="width: 20px">
            <div class="absolute border-t border-r border-default pointer-events-none" style="transform: rotate(45deg); width: 24px; height: 24px; left: -10px; top: 4px;"></div>
          </div>
        `)
        htmlAcc.push(html`
          <a class="block px-2 py-1 hover:text-primary" href="/p/db/${joinPath(this.hyperKey, ...acc, seg)}?container">${seg}</a>
        `)
        acc.push(seg)
      }
    }
    return html`
      <div class="inline-flex border border-default rounded bg-default px-2">
        ${htmlAcc}
      </div>
    `
  }

  renderCurrentData () {
    if (this.isEditingNew) {
      return this.renderNewRecord()
    } else if (this.record) {
      return this.renderRecord()
    } else if (this.records) {
      return this.renderRecords()
    } else {
      return html`
        <div><span class="spinner"></span></div>
      `
    }
  }

  renderRecords () {
    return html`
      <div>
        <div class="flex border rounded-t border-default items-center">
          <div class="px-3 py-2" style="flex: 0 0 20px" @click=${this.onToggleAllChecked}>
            <span class="far fa-fw ${this.selectedRecordKeys.length === this.records.length ? `fa-check-square` : `fa-square`}"></span>
          </div>
          <div class="px-2 py-2">
            <adb-button label="New record" class="mr-1" btn-class="px-2 py-0" ?disabled=${!this.dbDesc.writable} @click=${this.onClickNew}></adb-button>
            ${this.selectedRecordKeys.length > 1 ? html`
              <adb-button label="Delete" class="mr-1" btn-class="px-2 py-0" ?disabled=${!this.dbDesc.writable} @click=${this.onClickDeleteSelected}></adb-button>
              <adb-button label="Export" class="mr-1" btn-class="px-2 py-0" @click=${this.onClickExportSelected}></adb-button>
            ` : this.selectedRecordKeys.length === 1 ? html`
              <adb-button-group class="inline-flex mr-1">
                <adb-button label="Rename" btn-class="px-2 py-0" ?disabled=${!this.dbDesc.writable} @click=${this.onClickRenameSelected}></adb-button>
                <adb-button label="Clone" btn-class="px-2 py-0" ?disabled=${!this.dbDesc.writable} @click=${this.onClickCloneSelected}></adb-button>
                <adb-button label="Delete" btn-class="px-2 py-0" ?disabled=${!this.dbDesc.writable} @click=${this.onClickDeleteSelected}></adb-button>
              </adb-button-group>
              <adb-button label="Export" class="mr-1" btn-class="px-2 py-0" @click=${this.onClickExportSelected}></adb-button>
            </div>
            ` : html`
              <adb-button label="Import" class="mr-1" btn-class="px-2 py-0" ?disabled=${!this.dbDesc.writable} @click=${this.onClickImport}></adb-button>
            `}
          </div>
        </div>
        ${repeat(this.records, r => r.key, (record, i) => html`
          <a
            class="
              flex border border-t-0 border-default items-center
              ${this.selectedRecordKeys.includes(record.key) ? 'bg-yellow-100 hover:bg-yellow-50' : 'bg-default hover:bg-default-2'}
              cursor-pointer ${i === this.records.length - 1 ? 'rounded-b' : ''} tabular-nums"
            href="${joinPath(this.currentPath, record.key)}${record.isContainer ? '?container' : ''}"
          >
            <div class="px-3 py-2" style="flex: 0 0 20px" @click=${e => this.onToggleCheckmark(e, record.key)}>
              <span class="far fa-fw ${this.selectedRecordKeys.includes(record.key) ? `fa-check-square` : `fa-square`}"></span>
            </div>
            <div class="px-3 py-2 text-sm border-l border-default truncate" style="flex: 0 0 250px">${record.key}</div>
            <div class="flex-1 px-3 py-2 text-sm border-l border-default truncate">
              ${record.isContainer ? html`<span class="fas fa-fw fa-angle-right"></span>` : JSON.stringify(record.value)}
            </div>
          </a>
        `)}
        ${this.records?.length === 0 ? html`
          <div class="flex border border-default border-t-0 bg-default rounded-b px-4 py-2">No records found.</div>
        ` : ''}
      </div>
    `
  }

  renderRecord () {
    return html`
      <div class="flex">
        <div class="flex-1 mr-2">
          <div class="flex border rounded-t border-default items-center">
            <div class="px-3 py-2 flex-1">
              <adb-button btn-class="px-2 py-0" ?primary=${this.hasChanges} label="Save changes" icon="fas fa-save" ?disabled=${!this.dbDesc.writable && !this.hasChanges} @click=${this.onClickSave}></adb-button>
              <adb-button btn-class="px-2 py-0" transparent label="Delete" ?disabled=${!this.dbDesc.writable} @click=${this.onClickDelete}></adb-button>
            </div>
            <div class="px-3 py-2">
              <adb-button btn-class="px-2 py-0" transparent label="Rename" ?disabled=${!this.dbDesc.writable} @click=${this.onClickRename}></adb-button>
              <adb-button btn-class="px-2 py-0" transparent label="Clone" ?disabled=${!this.dbDesc.writable} @click=${this.onClickClone}></adb-button>
              <adb-button btn-class="px-2 py-0" transparent label="Export" @click=${this.onClickExport}></adb-button>
            </div>
          </div>
          <adb-code-textarea
            id="buffer"
            textarea-class="w-full px-4 py-3 font-mono rounded-b border border-default border-t-0 text-sm bg-default shadow-inner"
            textarea-style="min-height: calc(100vh - 160px)"
            ?disabled=${!this.dbDesc.writable}
            @keyup=${this.onRecordKeyup}
            value=${this.initialRecordValue}
          ></adb-code-textarea>
        </div>
        <div style="flex: 0 0 25vw">
          <div class="border border-default rounded mb-1">
            <div class="flex items-center">
              <div class="px-2 py-1.5 text-sm text-default-3" style="flex: 0 0 55px">Key</div>
              <div class="px-2 py-1.5 text-sm text-default-2 border-l border-default-2 flex-1 break-words">${this.record.key}</div>
            </div>
            <div class="flex items-center border-t border-default-2">
              <div class="px-2 py-1.5 text-sm text-default-3" style="flex: 0 0 55px">Path</div>
              <div class="px-2 py-1.5 text-sm text-default-2 border-l border-default-2 flex-1 break-words">${this.record.path}</div>
            </div>
            <div class="flex items-center border-t border-default-2">
              <div class="px-2 py-1.5 text-sm text-default-3" style="flex: 0 0 55px">URL</div>
              <div class="px-2 py-1.5 text-sm text-default-2 border-l border-default-2 flex-1 break-words">${this.record.url}</div>
            </div>
            <div class="flex items-center border-t border-default-2">
              <div class="px-2 py-1.5 text-sm text-default-3" style="flex: 0 0 55px">Seq</div>
              <div class="px-2 py-1.5 text-sm text-default-2 border-l border-default-2 flex-1">${this.record.seq}</div>
            </div>
          </div>
        </div>
      </div>
    `
  }

  renderNewRecord () {
    return html`
      <div class="flex">
        <div class="flex-1 mr-2">
          <div class="flex border rounded-t border-default items-center">
            <div class="px-3 py-2 flex-1">
              <adb-button btn-class="px-2 py-0" label="Save record" icon="fas fa-save" @click=${this.onClickSaveNew}></adb-button>
            </div>
          </div>
          <div class="flex w-full px-4 py-3 font-mono border border-default border-t-0 text-sm bg-default shadow-inner">
            <span>/${this.isEditingNew.path.join('/')}${this.isEditingNew.path.length > 0 ? '/' : ''}</span>
            <input
              type="text"
              id="path"
              placeholder="key"
              class="flex-1 ml-2"
              required
            >
          </div>
          <textarea
            id="buffer"
            class="w-full px-4 py-3 font-mono rounded-b border border-default border-t-0 text-sm bg-default shadow-inner"
            style="min-height: calc(100vh - 220px)"
            spellcheck="false"
          >{}</textarea>
        </div>
        <div style="flex: 0 0 25vw">
        </div>
      </div>
    `
  }

  // events
  // =
  
  onToggleAllChecked (e) {
    e.preventDefault()
    if (this.selectedRecordKeys.length === this.records.length) {
      this.selectedRecordKeys = []
    } else {
      this.selectedRecordKeys = this.records.map(r => r.key)
    }
  }

  onToggleCheckmark (e, key) {
    e.preventDefault()
    e.stopPropagation()
    if (!this.selectedRecordKeys.includes(key)) {
      this.selectedRecordKeys = [...this.selectedRecordKeys, key]
    } else {
      this.selectedRecordKeys = [...this.selectedRecordKeys.filter(k => k !== key)]
    }
  }

  onClickNew () {
    emit(this, 'navigate-to', {detail: {url: `/p/db/${this.hyperKey}?new&path=/${this.currentDBPath.join('/')}`}})
  }

  onClickImport (e) {
    e.preventDefault()
    this.doImportPopup()
  }

  async onClickSave () {
    let parsed
    const value = this.querySelector('#buffer').value
    try {
      parsed = JSON.parse(value)
    } catch (e) {
      console.error(e)
      toast.create(e.toString(), 'error')
      return
    }
    try {
      await session.adb.recordPut(this.hyperKey, this.record.path, parsed)
    } catch (e) {
      console.error(e)
      toast.create(e.toString(), 'error')
      return
    }
    this.initialRecordValue = value
    this.hasChanges = false
    toast.create('Record saved', 'success')
  }

  async onClickSaveNew () {
    let parsed
    const path = this.querySelector('#path').value
    const value = this.querySelector('#buffer').value
    if (!path) {
      return toast.create('Key is required', 'error')
    }
    try {
      parsed = JSON.parse(value)
    } catch (e) {
      console.error(e)
      toast.create(e.toString(), 'error')
      return
    }
    try {
      await session.adb.recordPut(this.hyperKey, joinPath(...this.isEditingNew.path, path), parsed)
    } catch (e) {
      console.error(e)
      toast.create(e.toString(), 'error')
      return
    }
    this.initialRecordValue = value
    this.hasChanges = false
    toast.create('Record saved', 'success')
    emit(this, 'navigate-to', {detail: {url: `/p/db/${this.hyperKey}/${joinPath(...this.isEditingNew.path, path)}`}})
  }

  onClickRename () {
    this.doRenamePopup(this.record)
  }

  onClickRenameSelected () {
    const record = this.records.find(r => r.key === this.selectedRecordKeys[0])
    this.doRenamePopup(record)
  }

  onClickClone () {
    this.doClonePopup(this.record)
  }

  onClickCloneSelected () {
    const record = this.records.find(r => r.key === this.selectedRecordKeys[0])
    this.doClonePopup(record)
  }

  async onClickDelete () {
    await ConfirmPopup.create({message: 'Delete this record?'})
    try {
      await doDelete(this.hyperKey, this.record)
    } catch (e) {
      toast.create(e.toString(), 'error')
      console.error(e)
      return
    }
    toast.create('Record deleted')
    emit(this, 'navigate-to', {detail: {url: `/p/db/${this.hyperKey}/${this.currentDBPath.slice(0, -1).join('/')}`}})
  }

  async onClickDeleteSelected () {
    await ConfirmPopup.create({message: 'Delete the selected records?'})
    try {
      for (const key of this.selectedRecordKeys) {
        const record = this.records.find(r => r.key === key)
        if (record) await doDelete(this.hyperKey, record)
      }
    } catch (e) {
      toast.create(e.toString(), 'error')
      console.error(e)
      return
    }
    toast.create('Records deleted')
    this.load()
  }

  onClickExportSelected () {
    const records = this.records.filter(r => this.selectedRecordKeys.includes(r.key))
    doExport(this.hyperKey, records)
  }

  onClickExport () {
    doExport(this.hyperKey, [this.record])
  }

  onRecordKeyup (e) {
    this.hasChanges = (e.currentTarget.value !== this.initialRecordValue)
  }

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
}

customElements.define('adb-db-view', DbView)

async function doRename (hyperKey, record, newpath) {
  toast.create('Renaming, please wait...')
  let n = 0
  if (record.isContainer) {
    const children = (await session.adb.recordList(hyperKey, record.path)).records
    const oldpathParts = record.path.split('/').filter(Boolean)
    const newpathParts = newpath.split('/').filter(Boolean)
    for (const child of children) {
      const pathParts = child.path.split('/').filter(Boolean)
      const newchildpath = [...newpathParts, ...pathParts.slice(oldpathParts.length)].join('/')
      n++
      await session.adb.recordPut(hyperKey, newchildpath, child.value)
      await session.adb.recordDelete(hyperKey, child.path)
    }
  } else {
    n++
    await session.adb.recordPut(hyperKey, newpath, record.value)
    await session.adb.recordDelete(hyperKey, record.path)
  }
  toast.create(`Renamed ${n} ${pluralize(n, 'record')}`, 'success')
}

async function doClone (hyperKey, record, newpath) {
  toast.create('Cloning, please wait...')
  let n = 0
  if (record.isContainer) {
    const children = (await session.adb.recordList(hyperKey, record.path)).records
    const oldpathParts = record.path.split('/').filter(Boolean)
    const newpathParts = newpath.split('/').filter(Boolean)
    for (const child of children) {
      const pathParts = child.path.split('/').filter(Boolean)
      const newchildpath = [...newpathParts, ...pathParts.slice(oldpathParts.length)].join('/')
      n++
      await session.adb.recordPut(hyperKey, newchildpath, child.value)
    }
  } else {
    n++
    await session.adb.recordPut(hyperKey, newpath, record.value)
  }
  toast.create(`Cloned ${n} ${pluralize(n, 'record')}`, 'success')
}

async function doDelete (hyperKey, record) {
  toast.create('Deleting, please wait...')
  let n = 0
  if (record.isContainer) {
    const records = (await session.adb.recordList(hyperKey, record.path)).records
    for (const record of records) {
      n++
      await session.adb.recordDelete(hyperKey, record.path)
    }
  } else {
    n++
    await session.adb.recordDelete(hyperKey, record.path)
  }
  toast.create(`Deleted ${n} ${pluralize(n, 'record')}`, 'success')
}

async function doImport (hyperKey, path, file) {
  const fileStr = await readTextFile(file)
  const records = importFromJson(fileStr)
  for (const record of records) {
    await session.adb.recordPut(hyperKey, joinPath(path, record.path), record.value)
  }
  return records.length
}

async function doExport (hyperKey, records) {
  const finalArr = []
  for (const record of records) {
    if (record.isContainer) {
      const children = await (await session.adb.recordList(hyperKey, record.path)).records
      finalArr = finalArr.concat(children)
    } else {
      finalArr.push(record)
    }
  }
  exportAsJson(finalArr)
}

function toShallow (currentPath, records) {
  const containers = new Set()
  const shallow = []
  for (const record of records) {
    const keyParts = record.key.split('/')
    if (keyParts.length === 1) {
      shallow.push(record)
    } else {
      containers.add(keyParts[0])
    }
  }
  return [
    ...Array.from(containers, key => ({
      key,
      path: `/${joinPath(...currentPath, key)}`,
      isContainer: true
    })),
    ...shallow
  ]
}