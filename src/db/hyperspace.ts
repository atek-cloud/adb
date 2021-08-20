import { Client as HyperspaceClient } from 'hyperspace'
import { createWsProxy } from '@atek-cloud/node-rpc'

export let client: HyperspaceClient | undefined = undefined

export async function setup () {
  client = new HyperspaceClient(createWsProxy({api: 'atek.cloud/hypercore-api'}))
  await client.ready()

  console.log('Hyperspace daemon connected, status:')
  console.log(await client.status())
}

export async function cleanup () {
  if (client) {
    await client.close()
  }
}
