# IBM COS Backup Vault — Setup procedure via the Marketplace

> **Goal**: protect a COS bucket by continuously backing up its objects into a Backup Vault, with the ability to restore them to another bucket at any point in time.
> **Scope**: requests submitted through the Marketplace (no CLI / Terraform action on the requester's side).

---

## 1. Key concepts

| Term | Role |
|---|---|
| **COS instance** | Container holding buckets and vaults. Vaults are provisioned inside an instance alongside buckets but are managed differently. |
| **Backup Vault** | Dedicated, isolated resource that stores backup data in an unmodifiable way. One vault can serve several buckets. |
| **Backup Policy** | Configuration set **on the source bucket** that targets a vault and sets the initial retention. Syncing starts immediately and runs continuously as long as the policy is active. |
| **Recovery Range** | Time window for which point-in-time restore coverage exists for a bucket. The end moves forward as data is synced; the start moves forward as retention expires data. |
| **Retention (DeleteAfterDays)** | Number of days of backup coverage kept in the vault. Set initially by the policy, adjustable afterwards on the vault. |
| **Restore** | Writes the object versions that were current at a chosen point in time from the vault to a **target bucket**. |

Target RPO is one hour or less. For the full concept reference (IAM, versioning, Object Lock comparison), see *Backup Vault — Concepts & architecture*.

---

## 2. Prerequisites and conditions

### 2.1 Source bucket (the bucket to protect)

| Condition | Why |
|---|---|
| **Versioning enabled** | Mandatory. A backup policy cannot be set on a non-versioned bucket. Versioning can only be enabled or suspended, never removed. |
| **No retention policy (Immutable Object Storage)** | Retention policies are incompatible with versioning, and therefore with Backup Vault. A bucket created with a retention policy cannot be backed up — it must be recreated. |
| **Object Lock is allowed** | Object Lock (COMPLIANCE / GOVERNANCE) is versioning-based and fully compatible. |
| **Lifecycle / expiration rules** | Allowed. Deleted objects remain restorable within the recovery range. |
| **Multipart uploads** | Only backed up once the upload is completed (finalized object version). |
| **Max 3 backup policies per bucket** | Typically one is enough. |

### 2.2 Backup Vault

- Created inside a **COS instance**. IBM supports vaults in the same instance, another instance, or another account/region; **the Marketplace form only lists vaults belonging to the same instance as the bucket.**
- Vault region is fixed at creation.
- Optional customer-managed encryption (Key Protect / HPCS) — set at creation, not changeable afterwards.
- The vault must not be deleted while policies target it: **deleting a vault irreversibly deletes all its recovery ranges.**

### 2.3 Target bucket (for restore)

- **Versioning enabled** (same rule as the source).
- No retention policy; no legacy bucket firewall.
- Can be the source bucket itself or any other bucket in an instance the vault is authorized on.

### 2.4 IAM (handled by the automation — for information)

| Layer | Requirement |
|---|---|
| Service-to-service (backup) | Source bucket's instance → vault's instance, action `cloud-object-storage.backup-vault.sync` (included in Backup Manager / Manager / Backup Reader). If revoked, the policy goes into error but existing backup data stays. |
| Service-to-service (restore) | Vault's instance → target bucket's instance, action `cloud-object-storage.bucket.restore_sync` (Writer or Manager). |
| Users / access groups | **Backup Manager** or **Backup Reader** at **instance level** to see and manage vaults in the console. Bucket-scoped policies alone are not enough — the vault listing returns a 403. |

---

## 3. Step 1 — Create the Backup Vault

1. Open the Marketplace and go to the relevant **COS instance**.
2. Scroll to the bottom of the page to the **Backup Vault** section.
3. Click **Add Backup Vault**.
4. Fill in the **Description** (optional).
5. Click **Submit action**.
6. Wait for provisioning to complete (a few minutes). The vault then appears in the instance's Backup Vault section.

> ℹ️ **One vault per instance is usually enough.** Create additional vaults only for isolation needs (different retention, business separation, separate encryption key).

---

## 4. Step 2 — Create the Backup Policy on a bucket

The policy is configured from the existing **bucket creation / update form**. Two cases:

- **New bucket**: tick the option when ordering the bucket.
- **Existing bucket**: reopen the bucket's form and tick the option (the bucket must already be versioned — see §2.1).

### Procedure

1. Open the **Add Bucket** form (or the form of an existing bucket).
2. Scroll to the bottom of the form.
3. Tick **Enable Backup Vault**.
4. A dropdown lists the **Backup Vaults attached to the bucket's COS instance**: select the vault created in Step 1 (if there is only one, select it).
5. Fill in **Backup Vault retention days**: number of days of backup coverage to keep for this bucket.
6. Click **Submit** to send the transaction.

> ⚠️ **Initial sync**: when the policy becomes active, all existing data in the bucket is synced to the vault first. The recovery range is **not visible until this initial sync completes**; duration depends on bucket size. Progress (%) is reported on the policy status.

> ⚠️ **Retention**: removing the policy later does **not** delete backup data. The recovery range keeps shrinking according to retention and is only fully deleted once all data has expired.

---

## 5. Verification

| Check | Where | Expected |
|---|---|---|
| Vault created | COS instance → Backup Vault section | Vault listed, active |
| Policy active | Bucket → configuration | Vault selected, retention days set, status active (100 % after initial sync) |
| Coverage | Vault → Recovery ranges | One range per policy, with start/end times moving forward |

---

## 6. Restore (reference)

Restore is not done from the bucket form: it is triggered **from the vault** towards a **versioned target bucket**, at a chosen point in time within the recovery range. Only the object versions that were **current** at that time are restored — noncurrent versions are not. See *Backup Vault — Restore*.

---

## 7. FAQ

**Can I attach a bucket to a vault in another COS instance?**
Not through the Marketplace — the dropdown only shows vaults of the bucket's instance.

**What if I change the retention days?**
The new value applies to the recovery range going forward; the start of the range moves accordingly at the next expiry cycle.

**My bucket has a retention policy — can I enable backup?**
No. Retention policies exclude versioning, which is mandatory. Recreate the bucket with versioning (and Object Lock if immutability is required).

**Is the vault billed?**
Yes, vault storage is billed on top of the bucket. Size the retention accordingly.
