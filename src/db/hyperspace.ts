import { Client as HyperspaceClient } from 'hyperspace'
import websocket from 'websocket-stream'

export let client: HyperspaceClient | undefined = undefined

export async function setup () {
  client = new HyperspaceClient(websocket('ws://localhost:3000/_api/gateway?api=atek.cloud/hypercore-api'))
  await client.ready()

  console.log('Hyperspace daemon connected, status:')
  console.log(await client.status())
}

export async function cleanup () {
  if (client) {
    await client.close()
  }
}
