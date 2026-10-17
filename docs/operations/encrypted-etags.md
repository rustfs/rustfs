# Encrypted object ETags

**Use this when:** upgrading encrypted storage or investigating missing ETags
in object listings after an upgrade.

New encrypted writes use a keyed fingerprint for the ETag rather than the
plaintext MD5. Single-part and part ETags are 32-character hexadecimal values;
completed multipart ETags retain their digest-and-part-count form. They stay
stable for the stored object. Request MD5 and checksum validation still apply
to the original plaintext. Encryption and decryption formats do not change.

## Listings and older objects

ListObjects, ListObjectsV2, and ListObjectVersions omit the optional ETag field
for encrypted objects whose metadata does not attest to the protected format.
This includes encrypted objects written before the upgrade, preserved source
ETags without that proof, and completed encrypted multipart uploads. New
single-part encrypted writes carry the internal format marker and expose their
protected ETag in listings. Unencrypted listings retain their existing ETags.
Existing persistent listing snapshots are invalidated; the provider falls
back to verified storage until a new snapshot is built.

Clients that require an ETag for these older or multipart objects must issue an
authorized HeadObject request, including the SSE-C key when required. GET,
HEAD, and conditional requests continue to use the object's stored ETag.

## Replication

Bucket and site replication forward the stored ETag on `x-rustfs-source-etag`
(with its `x-minio-source-*` twin). A target that adopts it (RustFS, MinIO)
reports the fingerprint for the replica; a target that computes its own ETag
(AWS S3 and most compatible stores) reports the MD5 of the plaintext it
received, which never equals the fingerprint. The post-PUT replica ETag audit
therefore skips encrypted sources. Managed-SSE sources forward no checksum on
the replication PUT either, so a target that transforms their payload is not
detected. See
`docs/operations/replication-outbound-transport.md`.

The same mismatch affects finding an existing replica on a target that mints
its own version ids (AWS S3, Wasabi), which a heal, MRF retry or
existing-object resync does before writing again:

- A replica recorded in the source's target-version ledger is addressed by
  that id, on a 404 and on an AWS-style 400 for the source id alike. For an
  encrypted source the replica counts when its size matches the bytes
  replication sent (plaintext for SSE-S3 / SSE-KMS, ciphertext for SSE-C) and
  it was written no earlier than the source version.
- Without a ledger entry (for example a PUT whose response was lost), the
  replica is located through `ListObjectVersions` by the same size and
  write-time rule, after removing versions that other generations of the key
  already claim. Only a single remaining match is adopted and recorded in the
  ledger. Several matches are never resolved by picking one: convergence writes
  the replica again, and a version purge is refused with the usual backoff.
  The `null` version of an unversioned target is never matched this way, and
  a target that refuses the listing keeps the earlier current-version check,
  which writes the replica again.
- Plaintext sources keep exact ETag identity. A target that adopts the source
  ETag still matches encrypted sources by ETag first.

Two same-size generations of a key whose own ledgers were both lost can still
be confused, and a version purge with no ledger and no shape match reports the
replica as already gone. Delete a leftover replica on the target by hand if a
`replication-check` VersionFidelity report shows one.

## Existing data and migration

The upgrade does not rewrite existing object metadata or erase fingerprints
already copied to backups, logs, caches, or replicas. Restrict access to those
copies separately. Rolling back to an older server also restores its listing
behavior, so keep the upgraded listing protection on every serving node.

To obtain a protected ETag, read the object with its encryption credentials and
write it through the upgraded encryption path. Verify the resulting plaintext
and application references before retiring an old copy. A server-side copy
that preserves the source ETag is insufficient.

In a versioned bucket, a new write creates a new version; historical versions
keep their original metadata. Object Lock retention and legal holds still
apply. Do not bypass them or delete retained versions to migrate an ETag.
There is no automatic in-place migration of retained metadata in this upgrade.
Unfinished uploads also keep their original part metadata. Restart an upload
through the upgraded path when its legacy part fingerprints need to be replaced.
