import { Lazy } from 'https://cdn.jsdelivr.net/gh/bradbrown-llc/lazy@0.0.0/mod.ts'
import { AIQ } from 'https://cdn.jsdelivr.net/gh/bradbrown-llc/aiq@0.0.0/mod.ts'
import { Snail } from 'https://cdn.jsdelivr.net/gh/bradbrown-llc/snail@0.0.0/mod.ts'
import { KvCache, Fallbacks } from 'https://cdn.jsdelivr.net/gh/bradbrown-llc/kvcache@0.0.1-vertigo/mod.ts'
import { Toad } from 'https://cdn.jsdelivr.net/gh/bradbrown-llc/toad@0.0.5-vertigo/mod.ts'

const replacer = (_:unknown,v:unknown)=>typeof v=='bigint'?''+v:v

export class KvVertigo {

    kv:Deno.Kv
    toad:Toad
    out:AIQ<string>
    err:AIQ<Error>

    constructor(kv:Deno.Kv, key:Deno.KvKey, fallbacks:Fallbacks<number>) {
        this.kv = kv
        const kvCache = new KvCache<number>(this, key, fallbacks)
        this.toad = new Toad(kvCache)
        this.out = new AIQ<string>()
        this.err = new AIQ<Error>()
        ;(async () => { for await (const message of kvCache.out) (this as this).out.push(message) })()
        ;(async () => { for await (const error of kvCache.err) (this as this).err.push(error) })()
    }

    async get<T=unknown>(
        key:Deno.KvKey,
        options?:{ consistency?:Deno.KvConsistencyLevel
    }):Promise<Error|Deno.KvEntryMaybe<T>> {
        const stacky = new Error()
        const lazy:Lazy<Deno.KvEntryMaybe<T>> = () => this.kv.get(key, options)
        const snail = new Snail(lazy)
        snail.born
            .then(() => this.out.push(`KvVertigo.get getting key [${key}]`))
        snail.died
            .then(value => this.out.push(`KvVertigo.get got value ${JSON.stringify(value, replacer)} from key [${key}]`))
            .catch(reason => {
                const error = new Error(reason)
                error.message = `KvVertigo.get failed getting key [${key}]`
                if (reason instanceof Error) error.stack += (reason.stack ? `\n${reason.stack}` : '') + (stacky.stack??'')
                this.err.push(error)
                return error
            })
        return await this.toad.feed(snail).catch(reason => new Error(reason))
    }

    async getMany<T extends readonly unknown[]>(
        keys:{ [K in keyof T]: Deno.KvKey },
        options?:{ consistency?: Deno.KvConsistencyLevel },
    ):Promise<Error|{ [K in keyof T]: Deno.KvEntryMaybe<T[K]> }> {
        const stacky = new Error()
        const lazy:Lazy<{ [K in keyof T]: Deno.KvEntryMaybe<T[K]> }> = () => this.kv.getMany<typeof keys>(keys, options)
        const snail = new Snail(lazy)
        snail.born
            .then(() => this.out.push(`KvVertigo.getMany getting keys [${keys}]`))
        snail.died
            .then(value => this.out.push(`KvVertigo.getMany got values ${JSON.stringify(value, replacer)} from keys [${keys}]`))
            .catch(reason => {
                const error = new Error(reason)
                error.message = `KvVertigo.getMany failed getting keys [${keys}]`
                if (reason instanceof Error) error.stack += (reason.stack ? `\n${reason.stack}` : '') + (stacky.stack??'')
                this.err.push(error)
                return error
            })
        return await this.toad.feed(snail).catch(reason => new Error(reason))
    }

    async set(
        key:Deno.KvKey,
        value:unknown,
        options?:{ expireIn?: number }
    ):Promise<Error|Deno.KvCommitResult> {
        const stacky = new Error()
        const lazy:Lazy<Deno.KvCommitResult> = () => this.kv.set(key, value, options)
        const snail = new Snail(lazy)
        snail.born
            .then(() => this.out.push(`KvVertigo.set setting key [${key}] to value ${JSON.stringify(value, replacer)}`))
        snail.died
            .then(() => this.out.push(`KvVertigo.set set key [${key}] to value ${JSON.stringify(value, replacer)}`))
            .catch(reason => {
                const error = new Error(reason)
                error.message = `KvVertigo.set failed setting key [${key}] to value ${JSON.stringify(value, replacer)}`
                if (reason instanceof Error) error.stack += (reason.stack ? `\n${reason.stack}` : '') + (stacky.stack??'')
                this.err.push(error)
                return error
            })
        return await this.toad.feed(snail).catch(reason => new Error(reason))
    }

    atomic():{ commit:()=>Promise<Error|Deno.KvCommitResult|Deno.KvCommitError> }&Deno.AtomicOperation {
        const stacky = new Error()
        const atom = this.kv.atomic()
        const originalCommit = atom.commit
        const commit = async () => {
            const lazy:Lazy<Error|Deno.KvCommitError|Deno.KvCommitResult> = () => originalCommit.bind(atom)()
            const snail = new Snail(lazy)
            snail.born
                .then(() => this.out.push(`KvVertigo.atomic.commit committing`))
            snail.died
                .then(() => this.out.push(`KvVertigo.atomic.commit committed`))
                .catch(reason => {
                    const error = new Error(reason)
                    error.message = `KvVertigo.atomic.commit failed committing`
                    if (reason instanceof Error) error.stack += (reason.stack ? `\n${reason.stack}` : '') + (stacky.stack??'')
                    this.err.push(error)
                    return error
                })
            return await this.toad.feed(snail).catch(reason => new Error(reason))
        }
        return Object.assign(atom, { commit })
    }

    async delete(key: Deno.KvKey):Promise<Error|void> {
        const stacky = new Error()
        const lazy:Lazy<Error|void> = () => this.kv.delete(key)
        const snail = new Snail(lazy)
        snail.born
            .then(() => this.out.push(`KvVertigo.delete deleting key [${key}]`))
        snail.died
            .then(() => this.out.push(`KvVertigo.delete deleted key [${key}]`))
            .catch(reason => {
                const error = new Error(reason)
                error.message = `KvVertigo.delete failed deleting key [${key}]`
                if (reason instanceof Error) error.stack += (reason.stack ? `\n${reason.stack}` : '') + (stacky.stack??'')
                this.err.push(error)
                return error
            })
        return await this.toad.feed(snail).catch(reason => new Error(reason))
    }

    list<T=unknown>(selector:Deno.KvListSelector, options?:Deno.KvListOptions):AsyncIterableIterator<Error|Deno.KvEntry<T>>&{ cursor:string } {
        const stacky = new Error()
        const list = this.kv.list<T>(selector, options)
        const toad = this.toad
        const out = this.out
        const err = this.err
        const originalNext = list.next
        let i = 0
        return Object.assign(
            list,
            {
                async next():Promise<IteratorYieldResult<Error>|IteratorResult<Deno.KvEntry<T>, undefined>> {
                    const lazy = () => originalNext.bind(list)()
                    const snail = new Snail(lazy)
                    snail.born
                        .then(() => out.push(`KvVertigo.list getting next (${i}) of prefix [${'prefix' in selector ? selector.prefix : '???'}]`))
                    snail.died
                        .then(value => out.push(`KvVertigo.list got value ${JSON.stringify(value, replacer)} next (${i}) of prefix [${'prefix' in selector ? selector.prefix : '???'}]`))
                        .catch(reason => {
                            const error = new Error(reason)
                            error.message = `KvVertigo.list failed getting next (${i}) of prefix [${'prefix' in selector ? selector.prefix : '???'}]`
                            if (reason instanceof Error) error.stack += (reason.stack ? `\n${reason.stack}` : '') + (stacky.stack??'')
                            err.push(error)
                            return error
                        })
                    const result:IteratorYieldResult<Error>|IteratorResult<Deno.KvEntry<T>, undefined>
                        = await toad.feed(snail).catch<IteratorYieldResult<Error>>((reason:unknown) =>
                            ({ value: reason instanceof Error ? reason : new Error(String(reason)) }))
                    i++
                    return result
                }
            }
        )
    }

}