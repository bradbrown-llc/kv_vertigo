import { Fallbacks } from '../kvcache/mod.ts'
import { KvVertigo } from './mod.ts'


const kv = await Deno.openKv()
const machineId = 'ABC123'
const key = ['delay', 'kv', machineId]
const fallbacks:Fallbacks<number> = { value: 2500, expireIn: 5000 }
const kvv = new KvVertigo(kv, key, fallbacks)

;(async () => { for await (const message of kvv.out) console.log(message) })()
;(async () => { for await (const error of kvv.err) console.error(error) })()

// await kvv.get([undefined])

const atom = kvv.atomic()
for (let i = 0; i < 10; i++) atom.set([`foo-${i}`], i**2)
await atom.commit()

// iterate through all database items (regulated)
for await (const foo of kvv.list({ prefix: [] })) { foo }