import { Database, DB_PATH } from './schemas.js'
import { privateServerDb } from './index.js'
import { PermissionsError } from '../lib/errors.js'

const MY_SKEY = process.env.ATEK_ASSIGNED_SERVICE_KEY

export class Auth {
  constructor (public userKey: string, public serviceKey: string) {
  }

  async assertCanReadDatabase (dbId: string): Promise<void> {
    const p = new Policy(Decision.deny, `Not authorized to access ${dbId}`)
    const isAdmin = await privateServerDb?.isUserAdmin(this.userKey)
    p.allowIf(isAdmin)
    if (!isAdmin) {
      const dbRecord = await privateServerDb?.get<Database>([...DB_PATH, dbId])
      p.allowIf(!dbRecord?.value)
      if (dbRecord?.value) {
        p.allowIf(dbRecord.value.access === 'public')
        p.allowIf(dbRecord.value.owner?.userKey === this.userKey && dbRecord.value.owner?.serviceKey === this.serviceKey)
        p.allowIf(dbRecord.value.owner?.userKey === this.userKey && dbRecord.value.owner?.serviceKey === MY_SKEY)
      }
    }
    p.assert()
  }

  async assertCanWriteDatabaseRecord (oldv: Database|undefined, newv: Database|undefined): Promise<void> {
    const p = new Policy(Decision.allow)

    const isAdmin = await privateServerDb?.isUserAdmin(this.userKey)
    if (isAdmin) return // short-circuit: admin always allowed
    
    if (oldv) {
      p.denyIf(
        oldv.owner?.userKey !== this.userKey, // must be owning user
        `Can't configure a database owned by a different user`
      )
      p.denyIf(
        oldv.owner?.serviceKey !== MY_SKEY // must be adb's frontend, or
        && oldv.owner?.serviceKey !== this.serviceKey, // must be owning service
        `Can't configure a database owned by a different service`
      )
    }
    if (newv) {
      p.denyIf(
        newv.owner?.userKey !== this.userKey, // cannot change owning user
        `Can't change the owning user of a database`
      )

      if (oldv?.owner?.serviceKey !== newv.owner?.serviceKey) {
        const ownerServiceKeyOwningUserKey = newv.owner?.serviceKey ? await privateServerDb?.getServiceOwnerKey(newv.owner?.serviceKey) : undefined
        p.denyIf(!ownerServiceKeyOwningUserKey, `Invalid owning service: ${newv.owner?.serviceKey}. Key does not map to any known services. Has the owning service been deleted?`)
        p.denyIf(
          ownerServiceKeyOwningUserKey !== this.userKey // can only assign ownership to services the user owns
          && newv.owner?.serviceKey !== MY_SKEY, // ...unless its to adb
          `Can't configure a database to be owned by a service owned by another user`
        )
      }
    }
    p.assert()
  }

  async assertCanEnumerateDatabasesOwnedByUser (userKey: string): Promise<void> {
    const p = new Policy(Decision.deny, 'Cannot enumerate databases owned by another user')
    const isAdmin = await privateServerDb?.isUserAdmin(this.userKey)
    p.allowIf(isAdmin)
    p.allowIf(this.userKey === userKey)
    p.assert()
  }
}

enum Decision {
  allow,
  deny
}

class Policy {
  constructor (public decision = Decision.deny, public reason: string|undefined = undefined) {}

  allowIf (b?: boolean, reason?: string) {
    if (b) {
      this.decision = Decision.allow
      if (reason && !this.reason) this.reason = reason
    }
  }
  denyIf (b?: boolean, reason?: string) {
    if (b) {
      this.decision = Decision.deny
      if (reason && !this.reason) this.reason = reason
    }
  }
  assert () {
    if (this.decision !== Decision.allow) throw new PermissionsError(this.reason || 'Not Authorized')
  }
}
