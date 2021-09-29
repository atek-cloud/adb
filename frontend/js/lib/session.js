import { create as createRpc } from '../../vendor/atek-browser-rpc.js'

export const adb = createRpc('/_api/adb')
export const frontend = createRpc('/_api/frontend')

window.adb = createRpc('/_api/adb')
window.frontend = createRpc('/_api/frontend')