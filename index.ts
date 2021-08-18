import { serve } from 'https://deno.land/std@0.101.0/http/server.ts'

const PORT = Number(Deno.env.get('SELF_ASSIGNED_PORT'))
const server = serve({ port: PORT });
console.log(`ADB server running at: http://localhost:${PORT}/`);

(async () => {
  for await (const request of server) {
    request.respond({ status: 200, body: `Hello, world!` });
  }
})()

// @deno-types="./vendor/hyperspace-client.1.18.0.build.d.ts"
import HyperspaceClient from './vendor/hyperspace-client.1.18.0.build.js'
// @deno-types="./vendor/hyperbee.1.6.2.build.d.ts"
import Hyperbee from './vendor/hyperbee.1.6.2.build.js'
import { createWsStream } from './vendor/ws-stream.ts'

(async () => {
  const s = await createWsStream(`ws://localhost:3000/_api/gateway?api=atek.cloud/hypercore-api`)
  s.on('error', console.log)
  const client = new HyperspaceClient(s)
  await client.ready()
  console.log(await client.status())

  const bee = new Hyperbee(client.corestore().get(), {
    keyEncoding: 'utf8',
    valueEncoding: 'utf8'
  })
  await bee.ready()
  console.log('new bee:', bee.feed.key.toString('hex'))
  console.log('get foo:', await bee.get('foo'))
  console.log('put foo:', await bee.put('foo', 'hello!'))
  console.log('get foo:', await bee.get('foo'))
})()
